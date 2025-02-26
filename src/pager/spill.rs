use std::io::{Cursor, Read, Write};

use zerocopy::{IntoBytes, TryFromBytes};
use zerocopy_derive::*;

use super::{
    page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageId, PageKind},
    IPager, PagerResult,
};

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct VarData {
    header: VarHeader,
    in_page: [u8]
}

impl VarData {
    pub fn size(&self) -> u64 {
        self.header.total_size
    }
    
    pub fn has_spilled(&self) -> bool {
        self.header.has_spilled()
    }

    pub fn set<Pager: IPager>(&mut self, data: &[u8], pager: &Pager) -> PagerResult<()> {
        self.header = write_var_data(data, &mut self.in_page, pager)?;
        Ok(())
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Contient les données nécessaires pour récupérer les données d'une taille dynamique.
pub struct VarHeader {
    /// La taille totale de la donnée
    pub total_size: u64,
    /// La taille en page
    pub in_page_size: u64,
    /// Tête de la liste chaînée des pages de débordement
    pub spill_page_id: OptionalPageId,
}

impl VarHeader {
    pub fn has_spilled(&self) -> bool {
        self.in_page_size < self.total_size
    }

}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
#[allow(dead_code)]
enum SpillKind {
    Spill = PageKind::Overflow as u8
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Page de débordement sans copie. 
pub struct SpillPage {
    kind: SpillKind,
    in_page_size: u64,
    next: OptionalPageId,
    body: [u8]
}

impl SpillPage {
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
        page.as_mut().as_mut_bytes()[0] = PageKind::Overflow as u8;
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
pub fn free_overflow_pages<Pager: IPager>(head: PageId, pager: &Pager) -> PagerResult<()> {
    let mut current = Some(head);

    while let Some(pid) = current {
        let raw = pager.get_page(&pid)?;
        let page = SpillPage::get(&raw);
        current = page.get_next();
        pager.delete_page(&pid)?;
    }

    Ok(())
}

/// Lit les données d'une taille dynamique dans une région d'une page.
pub fn read_dynamic_sized_data<Pager: IPager, W: Write>(
    header: &VarHeader,
    dest: &mut W,
    src: &[u8],
    pager: &Pager,
) -> PagerResult<()> {
    let in_page_data = &src[..header.in_page_size.try_into().unwrap()];
    dest.write_all(in_page_data)?;

    let mut current = header.spill_page_id;

    while let Some(pid) = current.as_ref() {
        let raw = pager.get_page(pid)?;
        let page = SpillPage::get(&raw);
        page.read(dest);
        current = page.get_next().into();
    }

    Ok(())
}

/// Ecris des données d'une taille dynamique dans une région d'une page.
///
/// Si les données ne peuvent être stockées intégralement dans la région,
/// alors la fonction réalise un débordement (Overflow) sur une à plusieurs pages.
pub fn write_var_data<Pager: IPager>(
    data: &[u8],
    dest: &mut [u8],
    pager: &Pager,
) -> PagerResult<VarHeader> {
    let total_size = data.len();
    let mut remaining: usize = total_size;

    let mut cursor = Cursor::new(data);
    let in_page_size = cursor.read(dest)?;

    remaining -= in_page_size;

    let mut ov_head: Option<PageId> = None;
    let mut prev_ov_pid: Option<PageId> = None;

    while remaining > 0 {
        let pid = pager.new_page()?;
        let mut page = pager.get_mut_page(&pid)?;
        let spill = SpillPage::new(&mut page);
 
        remaining -= spill.write(&mut cursor);

        if ov_head.is_none() {
            ov_head = Some(pid)
        }

        if let Some(prev_ov_pid) = prev_ov_pid {
            let mut prev_page = pager.get_mut_page(&prev_ov_pid)?;
            let prev_sp = SpillPage::get_mut(&mut prev_page);
            prev_sp.set_next(Some(pid));
        }

        prev_ov_pid = Some(pid);
    }

    // Si il reste des pages de débordement, on va les libérer, ça sert à rien de les garder.
    if let Some(tail) = prev_ov_pid {
        let mut tail_page = pager.get_mut_page(&tail)?;
        let tail_sp = SpillPage::get_mut(&mut tail_page);
        tail_sp.get_next().iter().try_for_each(|rem| {
            free_overflow_pages(*rem, pager)
        })?;
    }

    Ok(VarHeader {
        total_size: total_size.try_into().unwrap(),
        in_page_size: in_page_size.try_into().unwrap(),
        spill_page_id: ov_head.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io::Cursor, ops::{Deref, DerefMut}, rc::Rc};
    use rand::RngCore;
    use crate::{fs::in_memory::InMemoryFs, pager::{spill::read_dynamic_sized_data, page::{PageId, PageSize}, Pager, PagerOptions}};
    use super::write_var_data;

    #[test]
    pub fn test_spilled() -> Result<(), Box<dyn Error>>{
        let fs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(fs, "test", PageSize::new(4_096), PagerOptions::default())?;

        let mut data = unsafe {
            let mut data = Box::<[u8; 1_000_000]>::new_uninit().assume_init();
            rand::rng().fill_bytes(data.deref_mut());
            data
        };

        let expected = data.clone();
        
        let mut dest: [u8;100] = [0;100];

        let dsd_header: crate::pager::spill::VarHeader = write_var_data(data.deref(), &mut dest, &pager)?;
        assert!(dsd_header.in_page_size == dest.len().try_into().unwrap(), "la portion destinatrice en taille restreinte doit être remplie à 100%");
        assert!(dsd_header.total_size == data.len().try_into().unwrap(), "la totalité des données doivent avoir été écrites dans le pager");
        assert!(dsd_header.spill_page_id == Some(PageId::from(1u64)).into(), "il doit y avoir eu du débordement");

        // Efface les données stockées dans le tampon.
        data.deref_mut().fill(0);

        read_dynamic_sized_data(
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