//pub mod stream;

use std::{
    io::{Cursor, Read, Write},
    mem::MaybeUninit,
    ops::Range,
};

use zerocopy::FromBytes;
use zerocopy_derive::*;

use crate::page::{
    AsMutPageSlice, AsRefPageSlice, IntoRefPageSlice, OptionalPageId, PageId, PageKind,
    RefPageSlice,
};
use crate::{
    knack::{buf::KnackBuf, CowKnack, Knack, KnackCell},
    page::{AsRefPage, InPage, PageSize, PageSlice},
    pager::IPager,
    result::Result,
    tag::{DataArea, JarTag},
    utils::Shift,
};

/// Représente une référence d'un truc dont le contenu peut avoir débordé ailleurs
pub enum MaybeSpilledRef<'a> {
    Unspilled(&'a Knack),
    Spilled(&'a Var<PageSlice>)
}

impl<'a> MaybeSpilledRef<'a> {
    /// Transforme une référence vers truc qui a peut-être débordé en un truc dont on est certain qu'il est chargé intégralement
    /// soit par un truc tamponné [KnackBuf], soit par un truc adossé à une tranche [KnackCell].
    pub fn assert_loaded<Pager>(self, pager: &Pager) -> Result<CowKnack<&'a PageSlice>>
    where
        Pager: IPager<'a>,
    {
        match self {
            Self::Unspilled(knack) => {
                let slice: &'a PageSlice = unsafe {std::mem::transmute(knack)};
                Ok(CowKnack::Borrow(KnackCell::from(slice)))
            },
            Self::Spilled(var) => {
                let mut buf: Vec<u8> = Vec::with_capacity(usize::try_from(var.len()).unwrap());
                var.read(&mut buf, pager)?;
                Ok(CowKnack::Owned(KnackBuf::from_bytes(buf)))
            }
        }
    }
}

/// Représente un truc dont le contenu peut avoir débordé ailleurs.
pub enum MaybeSpilled<Slice>
where
    Slice: AsRefPageSlice,
{
    Unspilled(KnackCell<Slice>),
    Spilled(Var<Slice>),
}

impl<Slice> MaybeSpilled<Slice>
where
    Slice: AsRefPageSlice,
{
    /// Transforme le truc qui a peut-être débordé en un truc dont on est certain qu'il est chargé intégralement
    /// soit par un truc tamponné [KnackBuf], soit par un truc adossé à une tranche [KnackCell].
    pub fn assert_loaded<'a, Pager>(self, pager: &Pager) -> Result<CowKnack<Slice>>
    where
        Pager: IPager<'a>,
    {
        match self {
            MaybeSpilled::Unspilled(knack_cell) => Ok(CowKnack::Borrow(knack_cell)),
            MaybeSpilled::Spilled(var) => {
                let mut buf: Vec<u8> = Vec::with_capacity(usize::try_from(var.len()).unwrap());
                var.read(&mut buf, pager)?;
                Ok(CowKnack::Owned(KnackBuf::from_bytes(buf)))
            }
        }
    }
}

impl<Slice> From<KnackCell<Slice>> for MaybeSpilled<Slice>
where
    Slice: AsRefPageSlice,
{
    fn from(value: KnackCell<Slice>) -> Self {
        Self::Unspilled(value)
    }
}


impl<'buf> From<Var<RefPageSlice<'buf>>> for MaybeSpilled<RefPageSlice<'buf>> {
    fn from(value: Var<RefPageSlice<'buf>>) -> Self {
        value.into_maybe_spilled()
    }
}

/// Représente un truc de taille variable.
pub struct Var<Slice>(Slice)
where
    Slice: AsRefPageSlice + ?Sized;

impl<'buf> Var<RefPageSlice<'buf>> {
    pub fn into_maybe_spilled(self) -> MaybeSpilled<RefPageSlice<'buf>> {
        if self.has_spilled() {
            MaybeSpilled::Spilled(self)
        } else {
            MaybeSpilled::Unspilled(KnackCell::from(self.into_content_slice()))
        }
    }

    fn into_content_slice(self) -> RefPageSlice<'buf> {
        let range = self.data_range();
        self.0.into_page_slice(range)
    }
}

impl<Slice> Var<Slice>
where
    Slice: AsRefPageSlice,
{
    pub fn from_owned_slice(slice: Slice) -> Self {
        Self(slice)
    }
}

impl<Slice> Var<Slice>
where
    Slice: AsRefPageSlice + ?Sized,
{
    pub const HEADER_RANGE: Range<usize> = 1..(1 + size_of::<VarMeta>());
    pub const DATA_BASE: usize = 1 + size_of::<VarMeta>();

    pub(crate) fn from_ref_slice(slice: &Slice) -> &Self {
        unsafe { std::mem::transmute(slice) }
    }

    pub fn len(&self) -> u64 {
        self.as_meta().total_size
    }

    pub fn has_spilled(&self) -> bool {
        self.as_meta().has_spilled()
    }

    pub fn copy_into<S2>(&self, dest: &mut Var<S2>) -> Result<()>
    where
        S2: AsMutPageSlice + ?std::marker::Sized,
    {
        *dest.as_mut_meta() = self.as_meta().clone();
        dest.borrow_mut_content()
            .copy_from_slice(self.borrow_content());
        Ok(())
    }

    /// Récupère l'ensemble du truc de taille variable.
    pub fn read<'a, Pager, Dest>(&self, dest: &mut Dest, pager: &Pager) -> Result<()>
    where
        Pager: IPager<'a>,
        Dest: Write,
    {
        read_var(self.as_meta(), dest, self.borrow_content(), pager)
    }

    fn data_range(&self) -> Range<usize> {
        let in_page_size = usize::try_from(self.as_meta().get_in_page_size()).unwrap();
        (0..in_page_size).shift(VarMeta::AREA.end)
    }

    fn borrow_content(&self) -> &PageSlice {
        &self.0.as_ref()[self.data_range()]
    }

    fn as_meta(&self) -> &VarMeta {
        VarMeta::ref_from_bytes(&self.0.as_bytes()[VarMeta::AREA]).unwrap()
    }
}

impl<Slice> Var<Slice>
where
    Slice: AsMutPageSlice + ?Sized,
{
    pub(crate) fn from_mut_slice(slice: &mut Slice) -> &mut Self {
        unsafe { std::mem::transmute(slice) }
    }

    pub fn set<'a, Pager>(&mut self, data: &Knack, pager: &Pager) -> Result<()>
    where
        Pager: IPager<'a> + ?std::marker::Sized,
    {
        *self.as_mut_meta() =
            write_var(data.as_bytes(), self.borrow_mut_data_in_page_space(), pager)?;
        Ok(())
    }

    fn borrow_mut_content(&mut self) -> &mut [u8] {
        let range = self.data_range();
        &mut self.0.as_mut()[range]
    }

    fn borrow_mut_data_in_page_space(&mut self) -> &mut [u8] {
        &mut self.0.as_mut()[Self::DATA_BASE..]
    }

    fn as_mut_meta(&mut self) -> &mut VarMeta {
        VarMeta::mut_from_bytes(&mut self.0.as_mut_bytes()[VarMeta::AREA]).unwrap()
    }
}

impl<Slice> AsRef<PageSlice> for Var<Slice>
where
    Slice: AsRefPageSlice + ?Sized,
{
    fn as_ref(&self) -> &PageSlice {
        self.borrow_content()
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone)]
#[repr(C, packed)]
/// Contient les données nécessaires pour récupérer les données d'une taille dynamique.
pub struct VarMeta {
    /// La taille totale de la donnée
    pub total_size: u64,
    /// La taille en page
    pub in_page_size: u64,
    /// Tête de la liste chaînée des pages de débordement
    pub spill_page_id: OptionalPageId,
}

impl DataArea for VarMeta {
    const AREA: Range<usize> = 0..size_of::<Self>();
}

impl VarMeta {
    pub fn has_spilled(&self) -> bool {
        self.in_page_size < self.total_size
    }

    pub fn get_in_page_size(&self) -> u64 {
        self.in_page_size
    }

    pub fn get_total_size(&self) -> u64 {
        self.total_size
    }

    pub fn get_spill_page(&self) -> Option<PageId> {
        self.spill_page_id.into()
    }
}

pub struct SpillPage<Page>(Page)
where
    Page: AsRefPageSlice;

impl<Page> SpillPage<Page>
where
    Page: AsRefPage,
{
    pub fn tag(&self) -> &JarTag {
        self.0.tag()
    }
}

impl<Page> SpillPage<Page>
where
    Page: AsRefPageSlice,
{
    pub fn try_from(page: Page) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into().unwrap();
        PageKind::Spill.assert(kind).map(|_| Self(page))
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.as_meta().get_next()
    }

    pub fn read<W: Write>(&self, dest: &mut W) {
        dest.write_all(self.borrow_body()).unwrap();
    }

    fn borrow_body(&self) -> &[u8] {
        let area = (0..usize::from(self.as_meta().get_in_page_size())).shift(SpillMeta::AREA.end);
        &self.0.as_bytes()[area]
    }

    fn as_meta(&self) -> &SpillMeta {
        SpillMeta::ref_from_bytes(&self.0.as_bytes()[SpillMeta::AREA]).unwrap()
    }
}

impl<Page> SpillPage<Page>
where
    Page: AsMutPageSlice,
{
    pub fn new(page: Page) -> Self {
        let mut page = Self(page);
        page.as_uinit_meta().write(Default::default());
        page
    }

    pub fn write<R: Read>(&mut self, src: &mut R) -> usize {
        let written = src.read(&mut self.borrow_mut_available_body()).unwrap();
        self.as_mut_meta()
            .set_in_page_size(written.try_into().unwrap());
        written
    }

    pub fn set_next(&mut self, next: Option<PageId>) {
        self.as_mut_meta().set_next(next);
    }

    fn borrow_mut_available_body(&mut self) -> &mut [u8] {
        &mut self.0.as_mut_bytes()[SpillMeta::AREA.end..]
    }

    fn as_uinit_meta(&mut self) -> &mut MaybeUninit<SpillMeta> {
        unsafe { std::mem::transmute(self.as_mut_meta()) }
    }

    fn as_mut_meta(&mut self) -> &mut SpillMeta {
        SpillMeta::mut_from_bytes(&mut self.0.as_mut_bytes()[SpillMeta::AREA]).unwrap()
    }
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
pub struct SpillMeta {
    in_page_size: PageSize,
    next: OptionalPageId,
}

impl Default for SpillMeta {
    fn default() -> Self {
        Self {
            in_page_size: Default::default(),
            next: None.into(),
        }
    }
}

impl DataArea for SpillMeta {
    const AREA: Range<usize> = InPage::<Self>::AREA;
}

impl SpillMeta {
    pub fn get_in_page_size(&self) -> PageSize {
        self.in_page_size
    }

    pub fn set_in_page_size(&mut self, in_page_size: PageSize) {
        self.in_page_size = in_page_size
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.next.into()
    }

    pub fn set_next(&mut self, next: Option<PageId>) {
        self.next = next.into()
    }
}

/// Libère toutes les pages de débordement de la liste chaînée.
pub fn free_overflow_pages<'a, Pager: IPager<'a> + ?std::marker::Sized>(
    head: PageId,
    pager: &Pager,
) -> Result<()> {
    let mut current = Some(pager.tag().in_page(head));

    while let Some(tag) = current {
        let page = pager.borrow_element(&tag).and_then(SpillPage::try_from)?;
        current = page.get_next().map(|pid| pager.tag().in_page(pid));
        pager.delete_element(&tag)?;
    }

    Ok(())
}

/// Lit les données d'une taille dynamique dans une région d'une page.
pub fn read_var<'a, Pager: IPager<'a>, W: Write>(
    meta: &VarMeta,
    dest: &mut W,
    src: &[u8],
    pager: &Pager,
) -> Result<()> {
    let in_page_data = &src[..meta.in_page_size.try_into().unwrap()];
    dest.write_all(in_page_data)?;

    let mut current = meta.spill_page_id;

    while let Some(tag) = current.as_ref().map(|pid| pager.tag().in_page(pid)) {
        let page = pager.borrow_element(&tag).and_then(SpillPage::try_from)?;
        page.read(dest);
        current = page.get_next().into();
    }

    Ok(())
}

/// Ecris des données d'une taille dynamique dans une région d'une page.
///
/// Si les données ne peuvent être stockées intégralement dans la région,
/// alors la fonction réalise un débordement (Overflow) sur une à plusieurs pages.
pub fn write_var<'a, Pager: IPager<'a> + ?std::marker::Sized>(
    data: &[u8],
    dest: &mut [u8],
    pager: &Pager,
) -> Result<VarMeta> {
    let total_size = data.len();
    let mut remaining: usize = total_size;

    let mut cursor = Cursor::new(data);
    let in_page_size = cursor.read(dest)?;

    remaining -= in_page_size;

    let mut ov_head: Option<JarTag> = None;
    let mut prev_ov_pid: Option<JarTag> = None;

    while remaining > 0 {
        let mut spill = pager.new_element().map(SpillPage::new)?;

        remaining -= spill.write(&mut cursor);

        if ov_head.is_none() {
            ov_head = Some(*spill.tag())
        }

        if let Some(prev_ov_pid) = prev_ov_pid {
            let mut prev_page = pager
                .borrow_mut_element(&prev_ov_pid)
                .and_then(SpillPage::try_from)?;
            prev_page.set_next(Some(spill.tag().page_id));
        }

        prev_ov_pid = Some(*spill.tag());
    }

    // Si il reste des pages de débordement, on va les libérer, ça sert à rien de les garder.
    if let Some(tail) = prev_ov_pid {
        let tail_page = pager
            .borrow_mut_element(&tail)
            .and_then(SpillPage::try_from)?;
        tail_page
            .get_next()
            .iter()
            .try_for_each(|rem| free_overflow_pages(*rem, pager))?;
    }

    Ok(VarMeta {
        total_size: total_size.try_into().unwrap(),
        in_page_size: in_page_size.try_into().unwrap(),
        spill_page_id: ov_head.map(|tag| tag.page_id).into(),
    })
}

#[cfg(test)]
mod tests {
    use super::write_var;
    use crate::{page::PageId, pager::stub::new_stub_pager, var::read_var};
    use rand::RngCore;
    use std::{
        error::Error,
        io::Cursor,
        ops::{Deref, DerefMut},
    };

    #[test]
    pub fn test_spilled() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4_096>();

        let mut data = unsafe {
            let mut data = Box::<[u8; 1_000_000]>::new_uninit().assume_init();
            rand::rng().fill_bytes(data.deref_mut());
            data
        };

        let expected = data.clone();

        let mut dest: [u8; 100] = [0; 100];

        let dsd_header: crate::var::VarMeta = write_var(data.deref(), &mut dest, &pager)?;
        assert!(
            dsd_header.in_page_size == u64::try_from(dest.len()).unwrap(),
            "la portion destinatrice en taille restreinte doit être remplie à 100%"
        );
        assert!(
            dsd_header.total_size == u64::try_from(data.len()).unwrap(),
            "la totalité des données doivent avoir été écrites dans le pager"
        );
        assert!(
            dsd_header.get_spill_page() == Some(PageId::from(1u64)),
            "il doit y avoir eu du débordement"
        );

        // Efface les données stockées dans le tampon.
        data.deref_mut().fill(0);

        read_var(
            &dsd_header,
            &mut Cursor::new(data.deref_mut().as_mut_slice()),
            &dest,
            &pager,
        )?;

        assert_eq!(
            data.as_slice(),
            expected.as_slice(),
            "la donnée récupérée doit être identique à celle stockée"
        );

        Ok(())
    }
}

