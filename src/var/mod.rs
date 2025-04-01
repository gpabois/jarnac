//pub mod stream;

use std::{io::{Cursor, Read, Write}, ops::Range};

use zerocopy::{FromBytes, TryFromBytes};
use zerocopy_derive::*;


use crate::{knack::{buf::KnackBuf, CowKnack, Knack, KnackCell}, pager::IPager, result::Result, tag::JarTag};
use crate::page::{AsMutPageSlice, AsRefPageSlice, IntoRefPageSlice, OptionalPageId, PageId, PageKind, PageSlice, RefPageSlice};

/// Représente un truc dont le contenu peut avoir débordé ailleurs.
pub enum MaybeSpilled<Slice> where Slice: AsRefPageSlice {
    Unspilled(KnackCell<Slice>),
    Spilled(Var<Slice>)
}

impl<Slice> MaybeSpilled<Slice> where Slice: AsRefPageSlice {
    /// Transforme le truc qui a peut-être débordé en un truc dont on est certain qu'il est chargé en mémoire
    /// soit par un truc tamponné [KnackBuf], soit par un truc adossé à une tranche [KnackCell].
    pub fn into_cow_knack<'a, Pager>(self, pager: &Pager) -> Result<CowKnack<Slice>> where Pager: IPager<'a> {
        match self {
            MaybeSpilled::Unspilled(knack_cell) => Ok(CowKnack::Borrow(knack_cell)),
            MaybeSpilled::Spilled(var) => {
                let mut buf: Vec<u8> = Vec::with_capacity(usize::try_from(var.len()).unwrap());
                var.read(&mut buf, pager)?;
                Ok(CowKnack::Owned(KnackBuf::from_bytes(buf)))
            },
        }
    }
}

impl<Slice> From<KnackCell<Slice>> for MaybeSpilled<Slice> where Slice: AsRefPageSlice {
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
pub struct Var<Slice>(Slice) where Slice: AsRefPageSlice + ?Sized;

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

impl<Slice> Var<Slice> where Slice: AsRefPageSlice {
    pub fn from_owned_slice(slice: Slice) -> Self {
        Self(slice)
    }
}

impl<Slice> Var<Slice> where Slice: AsRefPageSlice + ?Sized {
    pub const HEADER_RANGE: Range<usize> = 1..(1+size_of::<VarMeta>());
    pub const DATA_BASE: usize = 1 + size_of::<VarMeta>();

    pub(crate) fn from_ref_slice(slice: &Slice) -> &Self {
        unsafe {
            std::mem::transmute(slice)
        }
    }

    pub fn len(&self) -> u64 {
        self.as_header().total_size
    }
    
    pub fn has_spilled(&self) -> bool {
        self.as_header().has_spilled()
    }

    pub fn copy_into<S2>(&self, dest: &mut Var<S2>) -> Result<()> where S2: AsMutPageSlice + ? Sized {
        *dest.as_mut_header() = self.as_header().clone();
        dest.borrow_mut_content().copy_from_slice(self.borrow_content());
        Ok(())
    }   

    /// Récupère l'ensemble du truc de taille variable.
    pub fn read<'a, Pager, Dest>(&self, dest: &mut Dest, pager: &Pager) -> Result<()> where Pager: IPager<'a>, Dest: Write{
        read_var(self.as_header(), dest, self.borrow_content(), pager)
    }

    fn data_range(&self) -> Range<usize> {
        let in_page_size = usize::try_from(self.as_header().get_in_page_size()).unwrap();
        Self::DATA_BASE..(Self::DATA_BASE + in_page_size)
    }

    fn borrow_content(&self) -> &PageSlice {
        &self.0.as_ref()[self.data_range()]
    }

    fn as_header(&self) -> &VarMeta {
        self.as_ref()
    }
}

impl<Slice> Var<Slice> where Slice: AsMutPageSlice + ?Sized {
    pub(crate) fn from_mut_slice(slice: &mut Slice) -> &mut Self {
        unsafe {
            std::mem::transmute(slice)
        }
    }

    pub fn set<Pager>(&mut self, data: &Knack, pager: &Pager) -> Result<()> where Pager: for<'a> IPager<'a> {
        *self.as_mut_header() = write_var(
            data.as_bytes(), 
            self.borrow_mut_data_in_page_space(), 
            pager
        )?;
        Ok(())
    }

    fn borrow_mut_content(&mut self) -> &mut PageSlice {
        let range = self.data_range();
        &mut self.0.as_mut()[range]
    }

    fn borrow_mut_data_in_page_space(&mut self) -> &mut PageSlice {
        &mut self.0.as_mut()[Self::DATA_BASE..]
    }

    fn as_mut_header(&mut self) -> &mut VarMeta {
        self.as_mut()
    }
}

impl<Slice> AsRef<[u8]> for Var<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn as_ref(&self) -> &[u8] {
        self.borrow_content()
    }
}

impl<Slice> AsRef<VarMeta> for Var<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn as_ref(&self) -> &VarMeta {
        VarMeta::ref_from_bytes(&self.0.as_ref()[Self::HEADER_RANGE]).unwrap()
    }
}

impl<Slice> AsMut<VarMeta> for Var<Slice> where Slice: AsMutPageSlice + ?Sized {
    fn as_mut(&mut self) -> &mut VarMeta {
        VarMeta::mut_from_bytes(&mut self.0.as_mut()[Self::HEADER_RANGE]).unwrap()
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

pub struct SpillPage<Page>(Page) where Page: AsRefPageSlice;

impl<Page> SpillPage<Page> where Page: AsRefPageSlice {
    pub fn try_from(page: Page) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into().unwrap();
        PageKind::Spill.assert(kind).map(|_| Self(page))
    }

    pub fn as_data(&self) -> &SpillPageData {
        self.as_ref()
    }
}

impl<Page> AsRef<SpillPageData> for SpillPage<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &SpillPageData {
        SpillPageData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}

impl<Page> AsMut<SpillPageData> for SpillPage<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut SpillPageData {
        SpillPageData::try_mut_from_bytes(self.0.as_mut()).unwrap()
    }
}

impl<Page> AsRef<[u8]> for SpillPage<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &[u8] {
        let in_page_size: usize = self.as_data().in_page_size.try_into().unwrap();
        &self.as_data().body[..in_page_size]
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
#[allow(dead_code)]
enum SpillKind {
    Spill = PageKind::Spill as u8
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Page de débordement sans copie. 
pub struct SpillPageData {
    kind: SpillKind,
    in_page_size: u64,
    next: OptionalPageId,
    body: [u8]
}

impl SpillPageData {
    #[inline]
    /// Récupère une référence sur la page de débordement.
    pub fn get<Page>(page: &Page) -> &Self
    where Page: AsRefPageSlice
    {
        Self::try_ref_from_bytes(page.as_ref()).unwrap()
    }

    #[inline]
    /// Récupère une référence mutable de la page de débordement.
    pub fn get_mut(page: &mut [u8]) -> &mut Self {
        Self::try_mut_from_bytes(page).unwrap()
    }

    /// Crée une nouvelle page de débordement.
    pub fn new<Page>(page: &mut Page) -> &mut Self 
    where Page: AsMutPageSlice
    {
        page.as_mut().fill(0);
        page.as_mut().as_mut_bytes()[0] = PageKind::Spill as u8;
        Self::try_mut_from_bytes(page.as_mut()).unwrap()
    }

    pub fn get_in_page_size(&self) -> u64 {
        self.in_page_size
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.next.into()
    }

    pub fn set_next(&mut self, next: Option<PageId>) {
        self.next = next.into()
    } 

    pub fn write<R: Read>(&mut self, src: &mut R) -> usize {
        let written = src.read(&mut self.body).unwrap();
        self.in_page_size = written.try_into().unwrap();
        written
    }

    pub fn read<W: Write>(&self, dest: &mut W) {
        let fragment = &self.body[..self.in_page_size.try_into().unwrap()];
        dest.write_all(fragment).unwrap();
    }
}

/// Libère toutes les pages de débordement de la liste chaînée.
pub fn free_overflow_pages<'a, Pager: IPager<'a>>(head: PageId, pager: &Pager) -> Result<()> {
    let mut current = Some(pager.tag().in_page(head));

    while let Some(tag) = current {
        let raw = pager.borrow_element(&tag)?;
        let page = SpillPageData::get(&raw);
        current = page.get_next().map(|pid| pager.tag().in_page(pid));
        pager.delete_element(&tag)?;
    }

    Ok(())
}

/// Lit les données d'une taille dynamique dans une région d'une page.
pub fn read_var<'a, Pager: IPager<'a>, W: Write>(
    header: &VarMeta,
    dest: &mut W,
    src: &[u8],
    pager: &Pager,
) -> Result<()> {
    let in_page_data = &src[..header.in_page_size.try_into().unwrap()];
    dest.write_all(in_page_data)?;

    let mut current = header.spill_page_id;

    while let Some(tag) = current.as_ref().map(|pid| pager.tag().in_page(pid)) {
        let raw = pager.borrow_element(&tag)?;
        let page = SpillPageData::get(&raw);
        page.read(dest);
        current = page.get_next().into();
    }

    Ok(())
}

/// Ecris des données d'une taille dynamique dans une région d'une page.
///
/// Si les données ne peuvent être stockées intégralement dans la région,
/// alors la fonction réalise un débordement (Overflow) sur une à plusieurs pages.
pub fn write_var<'a, Pager: IPager<'a>>(
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
        let mut page = pager.new_element()?;
        let spill = SpillPageData::new(&mut page);
 
        remaining -= spill.write(&mut cursor);

        if ov_head.is_none() {
            ov_head = Some(*page.tag())
        }

        if let Some(prev_ov_pid) = prev_ov_pid {
            let mut prev_page = pager.borrow_mut_element(&prev_ov_pid)?;
            let prev_sp = SpillPageData::get_mut(&mut prev_page);
            prev_sp.set_next(Some(page.tag().page_id));
        }

        prev_ov_pid = Some(*page.tag());
    }

    // Si il reste des pages de débordement, on va les libérer, ça sert à rien de les garder.
    if let Some(tail) = prev_ov_pid {
        let mut tail_page = pager.borrow_mut_element(&tail)?;
        let tail_sp = SpillPageData::get_mut(&mut tail_page);
        tail_sp.get_next().iter().try_for_each(|rem| {
            free_overflow_pages(*rem, pager)
        })?;
    }

    Ok(VarMeta {
        total_size: total_size.try_into().unwrap(),
        in_page_size: in_page_size.try_into().unwrap(),
        spill_page_id: ov_head.map(|tag| tag.page_id).into(),
    })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io::Cursor, ops::{Deref, DerefMut}};
    use rand::RngCore;
    use crate::{pager::stub::new_stub_pager, page::PageId, var::read_var};
    use super::write_var;

    #[test]
    pub fn test_spilled() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4_096>();

        let mut data = unsafe {
            let mut data = Box::<[u8; 1_000_000]>::new_uninit().assume_init();
            rand::rng().fill_bytes(data.deref_mut());
            data
        };

        let expected = data.clone();
        
        let mut dest: [u8;100] = [0;100];

        let dsd_header: crate::var::VarMeta = write_var(data.deref(), &mut dest, &pager)?;
        assert!(dsd_header.in_page_size == u64::try_from(dest.len()).unwrap(), "la portion destinatrice en taille restreinte doit être remplie à 100%");
        assert!(dsd_header.total_size == u64::try_from(data.len()).unwrap(), "la totalité des données doivent avoir été écrites dans le pager");
        assert!(dsd_header.get_spill_page() == Some(PageId::from(1u64)), "il doit y avoir eu du débordement");

        // Efface les données stockées dans le tampon.
        data.deref_mut().fill(0);

        read_var(
            &dsd_header, 
            &mut Cursor::new(data.deref_mut().as_mut_slice()), 
            &dest, 
            &pager
        )?;

        assert_eq!(
            data.as_slice(), 
            expected.as_slice(),
            "la donnée récupérée doit être identique à celle stockée"
        );

        Ok(())
    }
}