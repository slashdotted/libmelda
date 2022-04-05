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
pub mod adapter;
mod constants;
mod datastorage;
pub mod filesystemadapter;
pub mod flate2adapter;
pub mod melda;
pub mod memoryadapter;
mod revision;
mod revisiontree;
mod utils;
#[cfg(feature="solid")]
pub mod solidadapter;
#[cfg(feature="sqlitedb")]
pub mod sqliteadapter;
