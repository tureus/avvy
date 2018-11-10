
pub struct Buferator<'de> {
    pub buf: &'de [u8],
    pub offset: usize,
}

impl<'de> Buferator<'de> {
    pub fn new(buf: &'de [u8]) -> Self {
        Buferator{ buf, offset: 0}
    }

    pub fn call<T>(&mut self, bytes: usize, f: fn(&'de[u8]) -> T) -> Result<T,()> {
        assert!(self.offset + bytes <= self.buf.len());

        let res = f(&self.buf[self.offset..self.offset+bytes]);
        self.offset += bytes;
        Ok(res)
    }
}

#[test]
fn buferate_it() {
    let bytes = vec![1,2,3,4,5,6,7,8];
    let mut buf = Buferator::new(&bytes[..]);
    let val : String = buf.call(4, |buf: &[u8]| {
        println!("my buf! {:?}", buf);
        format!("got: {}", buf[0] + buf[1])
    }).unwrap();

    println!("val was {:?}", val);

    let _t : f32 =  buf.call( 2, |_: &[u8]| {
        0.0
    }).unwrap();
}