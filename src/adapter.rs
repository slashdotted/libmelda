// Melda - Delta State JSON CRDT
// Copyright (C) 2021-2022 Amos Brocco <amos.brocco@supsi.ch>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
use anyhow::Result;

pub trait Adapter: Send + Sync {
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>>;
    fn write_object(&self, key: &str, data: &[u8]) -> Result<()>;
    fn list_objects(&self, ext: &str) -> Result<Vec<String>>;
}
