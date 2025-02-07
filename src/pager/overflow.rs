use std::{
    io::{Cursor, Read, Seek, Write},
    ops::{Deref, DerefMut},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{
    page::{PageId, PageKind},
    IPager, PagerResult,
};

/// Contient les données nécessaires pour récupérer les données d'une taille dynamique.
pub struct DynamicSizedDataHeader {
    pub total_size: u64,
    pub in_page_size: u64,
    /// Tête de la liste chaînée des pages de débordement
    pub ov_head: Option<PageId>,
}

pub struct OverflowPage<Page> {
    in_page_size: u64,
    next: Option<PageId>,
    page: Page,
}

impl<Page> OverflowPage<Page> {
    const OVERFLOW_HEADER_SIZE: usize = 18;
    const OVERFLOW_BASE: usize = Self::OVERFLOW_HEADER_SIZE;
}

impl<Page> OverflowPage<Page>
where
    Page: Deref<Target = [u8]>,
{
    pub fn load(page: Page) -> PagerResult<Self> {
        let mut cursor = Cursor::new(page.deref());
        let kind = PageKind::try_from(cursor.read_u8()?)?;
        kind.assert(PageKind::Overflow)?;

        let in_page_size = cursor.read_u64::<LittleEndian>()?;
        let usize_in_page_size: usize =in_page_size.try_into().unwrap();
        assert!(usize_in_page_size <= page.deref().len() - Self::OVERFLOW_BASE, "{usize_in_page_size} > {0}", page.deref().len() - Self::OVERFLOW_BASE);

        let mut next: Option<PageId> = None;
        if cursor.read_u8()? == 1 {
            next = Some(cursor.read_u64::<LittleEndian>()?);
        }

        Ok(Self {
            in_page_size,
            next,
            page,
        })
    }

    pub fn read<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        let in_page_size: usize = self.in_page_size.try_into().unwrap();
        let content = &self.page.deref()[Self::OVERFLOW_BASE..(Self::OVERFLOW_BASE + in_page_size)];
        dest.write_all(content)
    }
}

impl<Page> OverflowPage<Page>
where
    Page: DerefMut<Target = [u8]>,
{
    pub fn new(page: Page) -> std::io::Result<Self> {
        let mut ov = Self {
            next: None,
            in_page_size: 0,
            page,
        };
        ov.initialise()?;
        Ok(ov)
    }

    fn initialise(&mut self) -> std::io::Result<()> {
        self.page.deref_mut().fill(0);

        let mut cursor = Cursor::new(self.page.deref_mut());
        cursor.write_u8(PageKind::Overflow as u8)?;
        cursor.write_u64::<LittleEndian>(self.in_page_size)?;

        if let Some(next) = self.next {
            cursor.write_u8(1)?;
            cursor.write_u64::<LittleEndian>(next)?;
        } else {
            cursor.write_u8(0)?;
            cursor.write_u64::<LittleEndian>(0)?
        }

        Ok(())
    }

    pub fn set_next(&mut self, next: Option<PageId>) -> std::io::Result<()> {
        self.next = next;
        let mut cursor = Cursor::new(self.page.deref_mut());
        
        cursor.seek(std::io::SeekFrom::Start(9))?;
        if let Some(next) = self.next {
            cursor.write_u8(1)?;
            cursor.write_u64::<LittleEndian>(next)?;
        } else {
            cursor.write_u8(0)?;
            cursor.write_u64::<LittleEndian>(0)?
        }

        Ok(())
    }

    fn set_in_page_size(&mut self, size: usize) -> std::io::Result<()> {
        assert!(size <= self.page.len() - Self::OVERFLOW_BASE);
        self.in_page_size = size.try_into().unwrap();
        let mut cursor = Cursor::new(self.page.deref_mut());
        cursor.seek(std::io::SeekFrom::Start(1))?;
        cursor.write_u64::<LittleEndian>(self.in_page_size)
    }

    pub fn write<Data: Read>(&mut self, data: &mut Data) -> std::io::Result<usize> {
        let content = &mut self.page.deref_mut()[Self::OVERFLOW_BASE..];
        let written = data.read(content)?;
        self.set_in_page_size(written)?;
        Ok(written)
    }
}

/// Libère toutes les pages de débordement de la liste chaînée.
pub fn free_overflow_pages<Pager: IPager>(head: PageId, pager: &Pager) -> PagerResult<()> {
    let mut current = Some(head);

    while let Some(pid) = current {
        let page = pager.get_page(&pid)?;
        current = OverflowPage::load(page)?.next;
        pager.delete_page(&pid)?;
    }

    Ok(())
}

/// Lit les données d'une taille dynamique dans une région d'une page.
pub fn read_dynamic_sized_data<Pager: IPager, W: Write>(
    header: &DynamicSizedDataHeader,
    dest: &mut W,
    src: &[u8],
    pager: &Pager,
) -> PagerResult<()> {
    let in_page_data = &src[..header.in_page_size.try_into().unwrap()];
    dest.write_all(in_page_data)?;

    let mut current = header.ov_head;

    while let Some(pid) = current {
        let page = pager.get_page(&pid).and_then(OverflowPage::load)?;
        page.read(dest)?;
        current = page.next;
    }

    Ok(())
}

/// Ecris des données d'une taille dynamique dans une région d'une page.
///
/// Si les données ne peuvent être stockées intégralement dans la région,
/// alors la fonction réalise un débordement (Overflow) sur une à plusieurs pages.
pub fn write_dynamic_sized_data<Pager: IPager>(
    data: &[u8],
    dest: &mut [u8],
    pager: &Pager,
) -> PagerResult<DynamicSizedDataHeader> {
    let total_size = data.len();
    let mut remaining: usize = total_size;

    let mut cursor = Cursor::new(data);
    let in_page_size = cursor.read(dest)?;

    remaining -= in_page_size;

    let mut ov_head: Option<PageId> = None;
    let mut prev_ov_pid: Option<PageId> = None;

    while remaining > 0 {
        let pid = pager.new_page()?;
        let page = pager.get_mut_page(&pid)?;
        let mut ov = OverflowPage::new(page)?;

        remaining -= ov.write(&mut cursor)?;

        if ov_head.is_none() {
            ov_head = Some(pid)
        }

        if let Some(prev_ov_pid) = prev_ov_pid {
            let mut prev_ov_page = pager
                .get_mut_page(&prev_ov_pid)
                .and_then(OverflowPage::load)?;
            prev_ov_page.set_next(Some(pid))?;
        }

        prev_ov_pid = Some(pid);
    }

    Ok(DynamicSizedDataHeader {
        total_size: total_size.try_into().unwrap(),
        in_page_size: in_page_size.try_into().unwrap(),
        ov_head,
    })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io::Cursor, ops::{Deref, DerefMut}, rc::Rc};

    use rand::RngCore;

    use crate::{fs::in_memory::InMemoryFs, pager::{overflow::read_dynamic_sized_data, Pager, PagerOptions}};

    use super::write_dynamic_sized_data;

    #[test]
    pub fn test_overflow() -> Result<(), Box<dyn Error>>{
        let fs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(fs, "test", 4_096, PagerOptions::default())?;

        let mut data = unsafe {
            let mut data = Box::<[u8; 1_000_000]>::new_uninit().assume_init();
            rand::rng().fill_bytes(data.deref_mut());
            data
        };

        let expected = data.clone();
        
        let mut dest: [u8;100] = [0;100];

        let dsd_header: crate::pager::overflow::DynamicSizedDataHeader = write_dynamic_sized_data(data.deref(), &mut dest, &pager)?;
        assert!(dsd_header.in_page_size == dest.len().try_into().unwrap(), "la portion destinatrice en taille restreinte doit être remplie à 100%");
        assert!(dsd_header.total_size == data.len().try_into().unwrap(), "la totalité des données doivent avoir été écrites dans le pager");
        assert!(dsd_header.ov_head == Some(0), "il doit y avoir eu du débordement");

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