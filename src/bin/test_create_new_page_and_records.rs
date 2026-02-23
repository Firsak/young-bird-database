use std::error::Error;

use young_bird_database::database_operations::file_processing::{
    self, table::PageRecordContent, types::ContentTypes,
};

fn main() -> Result<(), Box<dyn Error>> {
    let filename = "test_db_page.dat";
    let page_kbytes: u32 = 8;

    let _ = file_processing::writing::write_new_page(filename, 0, page_kbytes);
    let _ = file_processing::writing::write_new_page(filename, 1, page_kbytes);
    let _ = file_processing::writing::write_new_page(filename, 2, page_kbytes);

    let record_content_1: PageRecordContent = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int8(12),
        ContentTypes::Text("Some text".to_string()),
    ]);
    let record_content_2: PageRecordContent = PageRecordContent::new(vec![
        ContentTypes::Null,
        ContentTypes::Int8(44),
        ContentTypes::Text("Some extra text".to_string()),
    ]);
    file_processing::writing::add_new_record(filename, 0, page_kbytes, 1, record_content_1)?;
    file_processing::writing::add_new_record(filename, 0, page_kbytes, 2, record_content_2)?;

    let read_res_0 = file_processing::reading::read_page(filename, 0, page_kbytes)?;
    let read_res_1 = file_processing::reading::read_page(filename, 1, page_kbytes)?;
    let read_res_2 = file_processing::reading::read_page(filename, 2, page_kbytes)?;

    println!("{read_res_0:?}");
    println!("{read_res_1:?}");
    println!("{read_res_2:?}");

    return Ok(());
}
