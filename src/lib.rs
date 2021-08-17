#[macro_use]
mod util;

mod accessors {
    pub(crate) mod std;
}
mod header;
mod bitbash;
mod build;
mod read;
mod sources {
    pub(crate) mod flat;
    pub(crate) mod hashmap;
    pub(crate) mod gdbm;
}
mod write;

#[cfg(test)]
mod test;

pub use crate::accessors::std::{ StdNCDReadMutAccessor, StdNCDReadAccessor };
pub use crate::build::{ NCDBuildConfig, NCDBuild };
pub use crate::read::{ NCDFileReader, NCDReadAccessor };
pub use crate::util::{ NCDError, wrap_io_error };
pub use crate::write::NCDValueSource;

pub use crate::sources::flat::{ NCDFlatSource, NCDFlatConfig };
pub use crate::sources::gdbm::NCDGdbmSource;
pub use crate::sources::hashmap::NCDHashMapValueSource;
