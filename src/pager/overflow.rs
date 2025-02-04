use super::page::PageId;

pub struct OverflowResult {
    pub total_size: u64,
    pub in_page_size: u64,
    pub overflow_page: Option<PageId>
}
