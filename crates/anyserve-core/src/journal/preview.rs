//! Bounded BLOB reads on SQLx's SQLite worker thread. SQL `substr(content, ...)`
//! still materializes the full BLOB inside SQLite, so the function takes a rowid
//! and uses incremental BLOB I/O instead. No schema change or backfill is needed.

use libsqlite3_sys as ffi;
use sqlx::sqlite::LockedSqliteHandle;

use crate::runtime::api::INSPECTION_PREVIEW_BYTES;

pub(super) fn register(handle: &mut LockedSqliteHandle<'_>) -> Result<(), sqlx::Error> {
    // SAFETY: SQLx exclusively locks this live connection while registering.
    // SQLite owns the callback registration; it has no borrowed state or data.
    let code = unsafe {
        ffi::sqlite3_create_function_v2(
            handle.as_raw_handle().as_ptr(),
            c"anyserve_object_prefix".as_ptr(),
            1,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            std::ptr::null_mut(),
            Some(read_prefix),
            None,
            None,
            None,
        )
    };
    if code != ffi::SQLITE_OK {
        return Err(sqlx::Error::Protocol(format!(
            "register journal preview: SQLite error {code}"
        )));
    }
    Ok(())
}

unsafe extern "C" fn read_prefix(
    context: *mut ffi::sqlite3_context,
    _argc: i32,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite invokes this one-argument callback with valid context/argv
    // on the connection's worker thread. The read-only BLOB handle belongs to
    // that same connection and transaction and is closed on every success path.
    // The fixed buffer is large enough for the bounded read; SQLITE_TRANSIENT
    // copies it before returning. No Rust allocation or panic crosses the ABI.
    unsafe {
        let db = ffi::sqlite3_context_db_handle(context);
        let rowid = ffi::sqlite3_value_int64(*argv);
        let mut blob = std::ptr::null_mut();
        let code = ffi::sqlite3_blob_open(
            db,
            c"main".as_ptr(),
            c"journal_objects".as_ptr(),
            c"content".as_ptr(),
            rowid,
            0,
            &mut blob,
        );
        if code != ffi::SQLITE_OK {
            ffi::sqlite3_result_error_code(context, code);
            return;
        }
        let mut buffer = [0u8; INSPECTION_PREVIEW_BYTES];
        let size = ffi::sqlite3_blob_bytes(blob).min(INSPECTION_PREVIEW_BYTES as i32);
        let read = ffi::sqlite3_blob_read(blob, buffer.as_mut_ptr().cast(), size, 0);
        let close = ffi::sqlite3_blob_close(blob);
        if read != ffi::SQLITE_OK {
            ffi::sqlite3_result_error_code(context, read);
        } else if close != ffi::SQLITE_OK {
            ffi::sqlite3_result_error_code(context, close);
        } else {
            ffi::sqlite3_result_blob(
                context,
                buffer.as_ptr().cast(),
                size,
                ffi::SQLITE_TRANSIENT(),
            );
        }
    }
}
