use std::error::Error;

use young_bird_database::database_operations::file_processing::{self, table::PageHeader};

fn main() -> Result<(), Box<dyn Error>> {
    let filename = "test_db_page_header.dat";
    let page_kbytes = file_processing::KBYTES * 8;
    let page_number = 3;
    let page_write_res =
        file_processing::writing::write_new_page(filename, page_number, page_kbytes);
    match page_write_res {
        Ok(_) => {
            println!("Successfull write of page");

            let page_read_res =
                file_processing::reading::read_page(filename, page_number, page_kbytes);

            match page_read_res {
                Ok(page) => println!("Read page {page:?}"),
                Err(error) => println!("Error reading page {error}"),
            };
        }
        Err(error) => println!("Error writin page {error}"),
    };

    let _ = file_processing::writing::write_new_page(filename, 0, page_kbytes);
    let _ = file_processing::writing::write_new_page(filename, 1, page_kbytes);
    let _ = file_processing::writing::write_new_page(filename, 2, page_kbytes);

    let page_header: PageHeader = PageHeader::new(11, 11, 8888);
    let _ = file_processing::writing::write_page_header(filename, 1, page_header, page_kbytes);

    let read_res_0 = file_processing::reading::read_page(filename, 0, page_kbytes)?;
    let read_res_1 = file_processing::reading::read_page(filename, 1, page_kbytes)?;
    let read_res_2 = file_processing::reading::read_page(filename, 2, page_kbytes)?;

    println!("{read_res_0:?}");
    println!("{read_res_1:?}");
    println!("{read_res_2:?}");

    return Ok(());
}
