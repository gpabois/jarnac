
impl PartialEq<Knack> for Comparable<Knack> {
    fn eq(&self, other: &Knack) -> bool {
        self.raw_value() == other.raw_value()
    }
}


impl PartialOrd<Knack> for Comparable<Knack> {
    fn partial_cmp(&self, other: &Knack) -> Option<std::cmp::Ordering> {
        if self.kind().deref() != other.kind() {
            return None
        }

        let kind = self.kind();

        match (kind.is_float(), kind.is_signed(), kind.size()) {
            (true, _, 4) => self.cast::<f32>().partial_cmp(other.cast::<f32>()), 
            (true, _, 8) => self.cast::<f64>().partial_cmp(other.cast::<f64>()), 
            (_, true, 1) => self.cast::<i8>().partial_cmp(other.cast::<i8>()), 
            (_, false, 1) => self.cast::<u8>().partial_cmp(other.cast::<u8>()), 
            (_, true, 2) => self.cast::<i16>().partial_cmp(other.cast::<i16>()), 
            (_, false, 2) => self.cast::<u16>().partial_cmp(other.cast::<u16>()),
            (_, true, 4) => self.cast::<i32>().partial_cmp(other.cast::<i32>()), 
            (_, false, 4) => self.cast::<u32>().partial_cmp(other.cast::<u32>()),  
            (_, true, 8) => self.cast::<i64>().partial_cmp(other.cast::<i64>()), 
            (_, false, 8) => self.cast::<u64>().partial_cmp(other.cast::<u64>()), 
            (_, true, 16) => self.cast::<i128>().partial_cmp(other.cast::<i128>()), 
            (_, false, 16) => self.cast::<u128>().partial_cmp(other.cast::<u128>()),
            _ => None 
        }
    }
}