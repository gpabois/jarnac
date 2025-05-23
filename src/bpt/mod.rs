use std::{marker::PhantomData, ops::Deref, borrow::Borrow};

use descriptor::BPTreeDescriptor;
use interior::{BPlusTreeInterior, BPlusTreeInteriorMut, BPlusTreeInteriorRef};
use leaf::{BPlusTreeLeaf, BPlusTreeLeafMut, BPlusTreeLeafRef};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    cell::CellCapacity, error::{Error, ErrorKind}, knack::{
        kind::{EmcompassingKnackKind, GetKnackKind, KnackKind},
        marker::{kernel::AsKernelRef, sized::Sized, AsComparable, AsFixedSized, ComparableAndFixedSized},
        Knack,
    }, page::{AsRefPageSlice, MutPage, PageKind, PageSize, RefPageSlice}, pager::IPager, prelude::IntoKnackBuf, result::Result, tag::JarTag, utils::Valid, var::{MaybeSpilled, VarMeta}
};

pub mod descriptor;
pub mod interior;
pub mod leaf;

pub struct KnownBPlusTree<'nodes, Key, Value, Arena>
where
    Arena: IPager<'nodes>,
{
    _pht: PhantomData<(Key,Value)>,
    inner: BPlusTree<'nodes, Arena>
}

impl<'nodes, Key, Value, Arena> KnownBPlusTree<'nodes, Key, Value, Arena> 
where
    Arena: IPager<'nodes>,
    Key: IntoKnackBuf,
    Value: IntoKnackBuf
{
    pub fn search(&self, key: Key) -> Result<Option<MaybeSpilled<RefPageSlice<'nodes>>>> {
        let k = key.into_knack_buf();
        self.inner.search(k.borrow())
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<()> {
        let k = key.into_knack_buf();
        let v = value.into_knack_buf();
        self.inner.insert(k.borrow(), v.borrow())
    }
}

pub struct BPlusTree<'nodes, Arena>
where
    Arena: IPager<'nodes>,
{
    arena: &'nodes Arena,
    tag: JarTag,
}

impl<'nodes, Arena> BPlusTree<'nodes, Arena>
where
    Arena: IPager<'nodes>,
{
    pub fn new(
        arena: &'nodes Arena,
        args: BPlusTreeArgs,
    ) -> Result<Self>
    {
        let node_size: PageSize = arena.size_of().try_into().unwrap();
        let valid_definition = args.define(node_size).validate()?;

        let page = arena.new_element()?;
        let tag = *page.tag();
        BPTreeDescriptor::new(page, valid_definition)?;
        Ok(Self { arena, tag })
    }

    /// Recherche une valeur associée à la clé
    pub fn search(&self, key: &Knack) -> Result<Option<MaybeSpilled<RefPageSlice<'nodes>>>> {
        let maybe_tag = self.search_leaf(key)?;

        if let Some(tag) = maybe_tag {
            return self.borrow_leaf(&tag).map(|leaf| {
                leaf.into_value(
                    key,
                    self.as_descriptor().key_kind().as_fixed_sized(),
                    self.as_descriptor().value_kind(),
                )
            });
        }

        Ok(None)
    }

    /// Insère une nouvelle clé/valeur
    pub fn insert(&mut self, key: &Knack, value: &Knack) -> Result<()> {
        assert_eq!(
            key.kind(),
            self.as_descriptor().key_kind().as_kernel_ref(),
            "wrong key kind"
        );
        assert_eq!(
            value.kind(),
            self.as_descriptor().value_kind(),
            "wrong value kind"
        );

        let leaf_pid = match self.search_leaf(key)? {
            Some(pid) => pid,
            None => {
                let new_leaf = self.new_leaf()?;
                self.as_mut_descriptor().set_root(Some(new_leaf.tag().page_id));
                *new_leaf.tag()
            }
        };

        let mut leaf = self.borrow_mut_leaf(&leaf_pid)?;

        // si la feuille est pleine on va la diviser en deux.
        if leaf.is_full() {
            self.split(leaf.as_mut_page())?;
        }

        leaf.insert(
            <&ComparableAndFixedSized::<Knack>>::try_from(key).expect("key must be comparable"),
            value,
            self.as_descriptor().as_description(),
            self.arena,
        )?;


        self.as_mut_descriptor().inc_len();
        
        Ok(())
    }

    /// Divise un noeud
    ///
    /// Cette opération est utilisée lors d'une insertion si le noeud est plein.
    fn split(&mut self, page: &mut MutPage<'_>) -> Result<()> {
        let node_kind: BPTreeNodeKind = TryFrom::try_from(page.as_bytes()[0])?;

        match node_kind {
            BPTreeNodeKind::Interior => {
                let mut left = BPlusTreeInterior::try_from(page)?;

                // on ne divise pas un noeud intérieur qui n'est pas plein.
                if !left.is_full() {
                    return Ok(());
                }

                // on ajoute un nouveau noeud intérieur
                let mut right = self.new_interior()?;

                // on divise le le noeud en deux au niveau [K/2]
                let key = left.split_into(&mut right)?.to_owned();

                // récupère le parent du noeud à gauche
                // si aucun parent n'existe alors on crée un noeud intérieur.
                let parent_id = match left.parent() {
                    Some(parent_id) => self.tag.in_page(parent_id),
                    None => {
                        let parent_id = self.new_interior()?.tag().page_id;
                        self.as_mut_descriptor().set_root(Some(parent_id));
                        left.set_parent(Some(parent_id));
                        self.tag.in_page(parent_id)
                    }
                };

                right.set_parent(left.parent());
                let mut parent = self.borrow_mut_interior(&parent_id)?;

                self.insert_in_interior(
                    &mut parent,
                    *left.tag(),
                    <&ComparableAndFixedSized::<Knack>>::try_from(key.deref()).unwrap(),
                    *right.tag(),
                )?;

                Ok(())
            }

            BPTreeNodeKind::Leaf => {
                let mut left = BPlusTreeLeaf::try_from(page)?;

                // On ne divise pas un noeud qui n'est pas plein.
                if !left.is_full() {
                    return Ok(());
                }

                let mut right = self.new_leaf()?;

                let key = left.split_into(&mut right)?.to_owned();

                right.set_prev(Some(left.tag().page_id));
                left.set_next(Some(right.tag().page_id));

                let parent_id = match left.get_parent() {
                    Some(parent_id) => self.tag.in_page(parent_id),
                    None => {
                        let new_parent = self.new_interior()?;
                        self.as_mut_descriptor()
                            .set_root(Some(new_parent.tag().page_id));
                        left.set_parent(Some(new_parent.tag().page_id));
                        self.tag.in_page(new_parent.tag().page_id)
                    }
                };

                right.set_parent(left.get_parent());

                let mut parent = self.borrow_mut_interior(&parent_id)?;
                self.insert_in_interior(
                    &mut parent,
                    *left.tag(),
                    <&ComparableAndFixedSized::<Knack>>::try_from(key.deref()).unwrap(),
                    *right.tag(),
                )?;

                Ok(())
            }
        }
    }

    fn new_leaf(&self) -> Result<BPlusTreeLeafMut<'nodes>> {
        self.arena
            .new_element()
            .and_then(|page| BPlusTreeLeaf::new(page, self.as_descriptor().as_description()))
    }

    fn new_interior(&self) -> Result<BPlusTreeInteriorMut<'nodes>> {
        self.arena
            .new_element()
            .and_then(|page| BPlusTreeInterior::new(page, self.as_descriptor().as_description()))
    }

    fn borrow_leaf(&self, tag: &JarTag) -> Result<BPlusTreeLeafRef<'nodes>> {
        self.arena.borrow_element(tag).and_then(TryFrom::try_from)
    }

    fn borrow_mut_leaf(&mut self, tag: &JarTag) -> Result<BPlusTreeLeafMut<'nodes>> {
        self.arena
            .borrow_mut_element(tag)
            .and_then(TryFrom::try_from)
    }

    fn borrow_interior(&self, tag: &JarTag) -> Result<BPlusTreeInteriorRef<'nodes>> {
        self.arena.borrow_element(tag).and_then(TryFrom::try_from)
    }

    fn borrow_mut_interior(&mut self, tag: &JarTag) -> Result<BPlusTreeInteriorMut<'nodes>> {
        self.arena
            .borrow_mut_element(tag)
            .and_then(TryFrom::try_from)
    }

    /// Recherche une feuille contenant potentiellement la clé
    fn search_leaf(&self, key: &Knack) -> Result<Option<JarTag>> {
        let mut current = self.as_descriptor().root();

        // Le type de la clé passée en argument doit être celle supportée par l'arbre.
        assert_eq!(
            key.kind(),
            self.as_descriptor().key_kind().as_kernel_ref(),
            "wrong key type"
        );

        while let Some(tag) = current.as_ref().map(|&pid| self.tag.in_page(pid)) {
            if self.node_kind(&tag)? == BPTreeNodeKind::Leaf {
                return Ok(Some(tag));
            } else {
                let interior = self.borrow_interior(&tag)?;
                current = Some(interior.search_child(key.try_as_comparable().unwrap()))
            }
        }

        Ok(None)
    }

    /// Insère un triplet {gauche | clé | droit} dans le noeud intérieur.
    ///
    /// Split si le noeud est complet.
    fn insert_in_interior(
        &mut self,
        interior: &mut BPlusTreeInterior<MutPage<'_>>,
        left: JarTag,
        key: &ComparableAndFixedSized<Knack>,
        right: JarTag,
    ) -> Result<()> {
        let jar = self.tag;

        if interior.is_full() {
            self.split(interior.as_mut_page())?;
        }

        interior.insert(left, key, right)?;

        interior
            .parent()
            .iter()
            .map(move |&pid| jar.in_page(pid))
            .try_for_each(|parent_id| {
                let mut page = self.arena.borrow_mut_element(&parent_id)?;
                self.split(&mut page)
            })?;

        Ok(())
    }

    fn node_kind(&self, tag: &JarTag) -> Result<BPTreeNodeKind> {
        self.arena
            .borrow_element(tag)
            .and_then(|page| TryFrom::try_from(page.as_bytes()[0]))
    }
}

impl<'nodes, Arena> BPlusTree<'nodes, Arena>
where
    Arena: IPager<'nodes>,
{
    fn as_descriptor(&self) -> BPTreeDescriptor<Arena::Ref> {
        self.arena
            .borrow_element(&self.tag)
            .and_then(BPTreeDescriptor::try_from)
            .unwrap()
    }

    fn as_mut_descriptor(&self) -> BPTreeDescriptor<Arena::RefMut> {
        self.arena
            .borrow_mut_element(&self.tag)
            .and_then(BPTreeDescriptor::try_from)
            .unwrap()
    }
}

#[derive(PartialEq, Eq)]
#[repr(u8)]
pub(super) enum BPTreeNodeKind {
    #[allow(dead_code)]
    Interior = PageKind::BPlusTreeInterior as u8,
    #[allow(dead_code)]
    Leaf = PageKind::BPlusTreeLeaf as u8,
}

impl TryFrom<u8> for BPTreeNodeKind {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        let interior = PageKind::BPlusTreeInterior as u8;
        let leaf = PageKind::BPlusTreeLeaf as u8;

        if value == interior {
            Ok(BPTreeNodeKind::Interior)
        } else if value == leaf {
            Ok(BPTreeNodeKind::Leaf)
        } else {
            let kind = PageKind::try_from(value).expect("not a valid page kind");
            panic!("not a b+ tree node (kind={kind})")
        }
    }
}

#[derive(Clone, Copy)]
/// Les arguments à passer pour instancier un nouvel arbre B
pub struct BPlusTreeArgs
where {
    k: Option<CellCapacity>,
    key_kind: &'static KnackKind,
    value_kind: &'static KnackKind,
}

impl BPlusTreeArgs
{
    pub fn new<Key, Value>(k: Option<CellCapacity>) -> Self
    where
        Key: GetKnackKind,
        Key::Kind: AsComparable + AsFixedSized,
        Value: GetKnackKind + ?std::marker::Sized,
    {
        Self {
            k,
            key_kind: Key::kind().as_kernel_ref(),
            value_kind: Value::kind().as_kernel_ref(),
        }
    }
}

impl BPlusTreeArgs
{
    /// Prend les exigences et transforme cela en une définition des paramètres de l'arbre B+.
    pub fn define(self, page_size: PageSize) -> BPlusTreeDefinition {
        let k = self.k.unwrap_or_else(|| self.find_best_k(page_size));
        
        let available_value_size = BPlusTreeLeaf::<()>::compute_available_value_space_size(
            page_size,
            self.key_kind.try_as_fixed_sized().unwrap(),
            k,
        );

        let (flags, in_cell_value_size) = match self.value_kind.as_sized() {
            Sized::Fixed(sized) => {
                let value_size = u16::try_from(sized.outer_size()).unwrap();
                let will_spill = value_size > available_value_size;

                (
                    will_spill
                        .then_some(BPlusTreeDefinition::VAL_IS_VAR_SIZED)
                        .unwrap_or_default(),
                    std::cmp::min(value_size, available_value_size),
                )
            }
            Sized::Var(_) => (BPlusTreeDefinition::VAL_IS_VAR_SIZED, available_value_size),
        };

        BPlusTreeDefinition {
            k,
            flags,
            key: self.key_kind.as_kernel_ref().to_owned(),
            value: self.value_kind.as_kernel_ref().to_owned(),
            in_cell_value_size,
            page_size,
        }
    }

    /// On trouve MAX(K) pour K e [1..255] tel qu'on puisse rentrer dans un noeud intérieur et un noeud feuille.
    pub fn find_best_k(&self, page_size: PageSize) -> CellCapacity {
        (2..CellCapacity::MAX)
            .filter(|&k| {
                let available_value_size = BPlusTreeLeaf::<()>::compute_available_value_space_size(
                    page_size,
                    self.key_kind.try_as_fixed_sized().unwrap(),
                    k,
                );

                let value_size = self
                    .value_kind
                    .try_as_fixed_sized()
                    .map(|fxd| u16::try_from(fxd.outer_size()).unwrap())
                    .unwrap_or_else(|| available_value_size)
                    .min(available_value_size);

                if self.value_kind.try_as_var_sized().is_some() && usize::from(value_size) <= size_of::<VarMeta>() {
                    return false
                }

                let leaf_compliant = BPlusTreeLeaf::<()>::within_available_cell_space_size(
                    page_size,
                    self.key_kind.try_as_fixed_sized().unwrap(),
                    value_size,
                    k,
                );

                let interior_compliant = BPlusTreeInterior::<()>::within_available_cell_space_size(
                    page_size,
                    self.key_kind.try_as_fixed_sized().unwrap(),
                    k,
                );

                leaf_compliant && interior_compliant
            })
            .last()
            .expect("cannot find k")
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug)]
pub struct BPlusTreeDefinition {
    k: u8,
    flags: u8,
    key: EmcompassingKnackKind,
    value: EmcompassingKnackKind,
    in_cell_value_size: u16,
    page_size: PageSize,
}

impl BPlusTreeDefinition {
    pub const VAL_WILL_SPILL: u8 = 0b1;
    pub const VAL_IS_VAR_SIZED: u8 = 0b10;

    pub fn key_kind(&self) -> &KnackKind {
        self.key.deref()
    }

    pub fn value_kind(&self) -> &KnackKind {
        self.value.deref()
    }

    pub fn validate(self) -> Result<Valid<BPlusTreeDefinition>> {
        let key_kind = self
            .key_kind()
            .try_as_fixed_sized()
            .expect("the key kind must be fixed sized");

        let leaf_compliant = BPlusTreeLeaf::<()>::within_available_cell_space_size(
            self.page_size,
            key_kind,
            self.in_cell_value_size,
            self.k,
        );

        let interior_compliant = BPlusTreeInterior::<()>::within_available_cell_space_size(
            self.page_size,
            key_kind,
            self.k,
        );

        let valid = leaf_compliant && interior_compliant;

        let valid_value_requirements = if self.flags & BPlusTreeDefinition::VAL_IS_VAR_SIZED > 0 {
            self.in_cell_value_size >= u16::try_from(size_of::<VarMeta>()).unwrap()
        } else {
            if self.value_kind().try_as_fixed_sized().unwrap().outer_size() > self.in_cell_value_size.into() {
                false
            } else {
                true
            }
        };

        (valid && valid_value_requirements)
            .then_some(Valid(self))
            .ok_or_else(|| Error::new(ErrorKind::InvalidBPlusTreeDefinition))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use rand::Rng;

    use crate::{knack::marker::kernel::{AsKernelRef, IntoKernel}, pager::stub::StubPager, prelude::IntoKnackBuf};

    use super::{BPlusTree, BPlusTreeArgs};

    #[test]
    pub fn test_insert_var_sized_value() {
        let nodes = StubPager::<4096>::new();
        let args = BPlusTreeArgs::new::<u128, str>(None);

        let key = 18u128.into_knack_buf();
        
        let mut tree = BPlusTree::new(&nodes, args).unwrap();
        tree.insert(key.borrow(), &"test".into_knack_buf()).unwrap();

        let maybe_value = tree.search(key.borrow()).unwrap();
        assert!(maybe_value.is_some());
        let maybe_spilled = maybe_value.unwrap();
        
        let value = maybe_spilled.assert_loaded(&nodes).unwrap();
        assert_eq!(value.cast::<str>(), "test")
         
    }

    #[test]
    pub fn test_insert_fixed_sized_value() {
        let pager = StubPager::<4096>::new();
        let args = BPlusTreeArgs::new::<u128, u64>(None);

        let key = 18u128.into_knack_buf();
        let value = 19u64.into_knack_buf();
        
        let mut tree = BPlusTree::new(&pager, args).unwrap();
        tree.insert(key.borrow(), &value.into_kernel()).unwrap();

        let maybe_value = tree.search(key.borrow()).unwrap();
        assert!(maybe_value.is_some());
        let maybe_spilled = maybe_value.unwrap();
        
        let value = maybe_spilled.assert_loaded(&pager).unwrap();
        assert_eq!(value.cast::<u64>(), &19u64)
    }

    #[test]
    fn test_multiple_insert() {
        let mut rng = rand::rng();
        let pager = StubPager::<4096>::new();
        let args = BPlusTreeArgs::new::<u128, u64>(None);

        let mut tree = BPlusTree::new(&pager, args).unwrap();

        let mut key = 0u128.into_knack_buf();

        let values: Vec<_> = (0..1000usize).map(|_| rng.random_range(0..u64::MAX)).map(|i| i.into_knack_buf()).collect();

        for i in 0..1000u128 {
            key.cast_mut::<u128>().set(i);
            tree.insert(key.borrow(), &values[usize::try_from(i).unwrap()].as_kernel_ref()).inspect_err(|err| println!("{:#?}", err.backtrace)).unwrap();
        }

        let idx = 477u128;
        let value = tree.search(&477u128.into_knack_buf()).unwrap().unwrap().into_unspilled();
        assert_eq!(value.cast::<u64>(), values[usize::try_from(idx).unwrap()].cast::<u64>());
    }
}