pub struct BPlusTreeInterior<Page>(Page);


impl BPlusTreeInterior<()> {
    pub fn compute_cell_size(key: Sized<ValueKind>) -> PageSize {
        let content_size = u16::try_from(size_of::<PageId>()+ key.outer_size()).unwrap(); 
        Cells::compute_cell_size(content_size)
    }

    pub fn within_available_cell_space_size(page_size: PageSize, key: Sized<ValueKind>, k: CellCapacity) -> bool {
        let content_size = Self::compute_leaf_cell_size(key);
        let reserved = u16::try_from(size_of::<BPTreeInteriorMeta>()).unwrap();
        Cells::within_available_cell_space_size(page_size, reserved, content_size, k)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug)]
#[repr(C, packed)]
/// L'entête d'un noeud d'un arbre B+
pub struct BPTreeInteriorMeta {
    /// Pointeur vers le noeud parent
    pub(super) parent: OptionalPageId,
    /// Pointeur vers le noeud enfant le plus à droite
    pub(super) tail: OptionalPageId,
}
