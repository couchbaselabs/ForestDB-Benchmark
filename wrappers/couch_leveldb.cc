#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>

#include "leveldb/c.h"
#include "couch_db.h"

#define METABUF_MAXLEN (256)

struct _db {
    leveldb_t *db;
    leveldb_options_t *options;
    leveldb_readoptions_t *read_options;
    leveldb_writeoptions_t *write_options;
    char *filename;
};

static uint64_t cache_size = 0;
static uint64_t wbs_size = 4*1024*1024;
static int bloom_bits_per_key = 0;
static int compression = 0;

couchstore_error_t couchstore_set_cache(uint64_t size)
{
    cache_size = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_wbs_size(uint64_t size) {
    wbs_size = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_bloom(int bits_per_key) {
    bloom_bits_per_key = bits_per_key;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_compression(int opt) {
    compression = opt;
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db(const char *filename,
                                      couchstore_open_flags flags,
                                      Db **pDb)
{
    return couchstore_open_db_ex(filename, flags,
                                 NULL, pDb);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db_ex(const char *filename,
                                         couchstore_open_flags flags,
                                         const couch_file_ops *ops,
                                         Db **pDb)
{
    Db *ppdb;
    char *err;
    leveldb_filterpolicy_t *bloom;

    *pDb = (Db*)malloc(sizeof(Db));
    ppdb = *pDb;

    ppdb->filename = (char*)malloc(strlen(filename)+1);
    strcpy(ppdb->filename, filename);

    ppdb->options = leveldb_options_create();
    leveldb_options_set_create_if_missing(ppdb->options, 1);
    leveldb_options_set_compression(ppdb->options, compression);
    leveldb_options_set_write_buffer_size(ppdb->options, wbs_size);
    if (bloom_bits_per_key) {
        // set bloom filter
        bloom = leveldb_filterpolicy_create_bloom(bloom_bits_per_key);
        leveldb_options_set_filter_policy(ppdb->options, bloom);
    }

    if (cache_size) {
        leveldb_options_set_cache(ppdb->options,
                                  leveldb_cache_create_lru((uint64_t)cache_size));
    }

    leveldb_options_set_max_open_files(ppdb->options, 1000);
    ppdb->db = leveldb_open(ppdb->options, ppdb->filename, &err);

    ppdb->read_options = leveldb_readoptions_create();
    ppdb->write_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(ppdb->write_options, 1);

    return COUCHSTORE_SUCCESS;
}

couchstore_error_t couchstore_set_sync(Db *db, int sync)
{
    leveldb_writeoptions_set_sync(db->write_options, sync);
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_close_db(Db *db)
{
    leveldb_close(db->db);
    free(db->filename);
    free(db);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_db_info(Db *db, DbInfo* info)
{
    struct stat filestat;

    info->filename = db->filename;
    info->doc_count = 0;
    info->deleted_count = 0;
    info->header_position = 0;
    info->last_sequence = 0;

    stat(db->filename, &filestat);
    info->space_used = filestat.st_size;

    return COUCHSTORE_SUCCESS;
}

size_t _docinfo_to_buf(DocInfo *docinfo, void *buf)
{
    // [db_seq,] rev_seq, deleted, content_meta, rev_meta (size), rev_meta (buf)
    size_t offset = 0;

    memcpy((uint8_t*)buf + offset, &docinfo->rev_seq, sizeof(docinfo->rev_seq));
    offset += sizeof(docinfo->rev_seq);

    memcpy((uint8_t*)buf + offset, &docinfo->deleted, sizeof(docinfo->deleted));
    offset += sizeof(docinfo->deleted);

    memcpy((uint8_t*)buf + offset, &docinfo->content_meta,
           sizeof(docinfo->content_meta));
    offset += sizeof(docinfo->content_meta);

    memcpy((uint8_t*)buf + offset, &docinfo->rev_meta.size,
           sizeof(docinfo->rev_meta.size));
    offset += sizeof(docinfo->rev_meta.size);

    if (docinfo->rev_meta.size > 0) {
        memcpy((uint8_t*)buf + offset, docinfo->rev_meta.buf, docinfo->rev_meta.size);
        offset += docinfo->rev_meta.size;
    }

    return offset;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents(Db *db, Doc* const docs[], DocInfo *infos[],
        unsigned numdocs, couchstore_save_options options)
{
    unsigned i;
    uint16_t metalen;
    uint8_t *buf;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    wb = leveldb_writebatch_create();
    buf = (uint8_t*)malloc(sizeof(metalen) + METABUF_MAXLEN + DATABUF_MAXLEN);

    for (i=0;i<numdocs;++i){
        metalen = _docinfo_to_buf(infos[i], buf + sizeof(metalen));
        memcpy(buf, &metalen, sizeof(metalen));
        memcpy(buf + sizeof(metalen) + metalen, docs[i]->data.buf, docs[i]->data.size);

        leveldb_writebatch_put(wb, docs[i]->id.buf, docs[i]->id.size, (char*)buf,
                               sizeof(metalen) + metalen + docs[i]->data.size);

        infos[i]->db_seq = 0;
    }
    free(buf);
    leveldb_write(db->db, db->write_options, wb, &err);
    if (err) {
        printf("ERR %s\n", err);
    }
    assert(err == NULL);
    leveldb_writebatch_destroy(wb);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document(Db *db, const Doc *doc, DocInfo *info,
        couchstore_save_options options)
{
    return couchstore_save_documents(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
}

void _buf_to_docinfo(void *buf, size_t size, DocInfo *docinfo)
{
    size_t offset = 0;

    memcpy(&docinfo->rev_seq, (uint8_t*)buf + offset, sizeof(docinfo->rev_seq));
    offset += sizeof(docinfo->rev_seq);

    memcpy(&docinfo->deleted, (uint8_t*)buf + offset, sizeof(docinfo->deleted));
    offset += sizeof(docinfo->deleted);

    memcpy(&docinfo->content_meta, (uint8_t*)buf + offset,
           sizeof(docinfo->content_meta));
    offset += sizeof(docinfo->content_meta);

    memcpy(&docinfo->rev_meta.size, (uint8_t*)buf + offset,
           sizeof(docinfo->rev_meta.size));
    offset += sizeof(docinfo->rev_meta.size);

    if (docinfo->rev_meta.size > 0) {
        //docinfo->rev_meta.buf = (char *)malloc(docinfo->rev_meta.size);
        docinfo->rev_meta.buf = ((char *)docinfo) + sizeof(DocInfo);
        memcpy(docinfo->rev_meta.buf, (uint8_t*)buf + offset, docinfo->rev_meta.size);
        offset += docinfo->rev_meta.size;
    }else{
        docinfo->rev_meta.buf = NULL;
    }
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfo_by_id(Db *db, const void *id, size_t idlen, DocInfo **pInfo)
{
    char *err;
    void *value;
    size_t valuelen;
    size_t rev_meta_size;
    size_t meta_offset;

    value = leveldb_get(db->db, db->read_options, (char*)id, idlen, &valuelen, &err);

    meta_offset = sizeof(uint64_t)*1 + sizeof(int) + sizeof(couchstore_content_meta_flags);
    memcpy(&rev_meta_size, (uint8_t*)value + sizeof(uint16_t) + meta_offset,
           sizeof(size_t));

    *pInfo = (DocInfo *)malloc(sizeof(DocInfo) + rev_meta_size);
    (*pInfo)->id.buf = (char *)id;
    (*pInfo)->id.size = idlen;
    (*pInfo)->size = idlen + valuelen;
    (*pInfo)->bp = 0;
    (*pInfo)->db_seq = 0;
    _buf_to_docinfo((uint8_t*)value + sizeof(uint16_t), valuelen, (*pInfo));

    free(value);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_id(Db *db, const sized_buf ids[], unsigned numDocs,
        couchstore_docinfos_options options, couchstore_changes_callback_fn callback, void *ctx)
{
    size_t i;
    DocInfo *docinfo;
    char *err;
    void *value;
    size_t valuelen;
    size_t rev_meta_size, max_meta_size = 256;
    size_t meta_offset;

    meta_offset = sizeof(uint64_t)*1 + sizeof(int) +
                  sizeof(couchstore_content_meta_flags);
    docinfo = (DocInfo*)malloc(sizeof(DocInfo) + max_meta_size);

    for (i=0;i<numDocs;++i){
        value = leveldb_get(db->db, db->read_options,
                            ids[i].buf, ids[i].size,
                            &valuelen, &err);

        memcpy(&rev_meta_size, (uint8_t*)value + sizeof(uint16_t) + meta_offset,
               sizeof(size_t));
        if (rev_meta_size > max_meta_size) {
            max_meta_size = rev_meta_size;
            docinfo = (DocInfo*)realloc(docinfo, sizeof(DocInfo) + max_meta_size);
        }

        memset(docinfo, 0, sizeof(DocInfo));
        docinfo->id.buf = ids[i].buf;
        docinfo->id.size = ids[i].size;
        docinfo->size = ids[i].size + valuelen;
        docinfo->bp = 0;
        docinfo->db_seq = 0;
        _buf_to_docinfo((uint8_t*)value + sizeof(uint16_t), valuelen, docinfo);
        free(value);

        callback(db, docinfo, ctx);
    }

    free(docinfo);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_sequence(Db *db,
                                                   const uint64_t sequence[],
                                                   unsigned numDocs,
                                                   couchstore_docinfos_options options,
                                                   couchstore_changes_callback_fn callback,
                                                   void *ctx)
{
    // do nothing

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_document(Db *db,
                                            const void *id,
                                            size_t idlen,
                                            Doc **pDoc,
                                            couchstore_open_options options)
{
    char *err = NULL;
    void *value;
    size_t valuelen;

    value = leveldb_get(db->db, db->read_options, (char*)id, idlen, &valuelen, &err);
    if (err) {
        printf("ERR %s\n", err);
    }
    assert(err == NULL);

    *pDoc = (Doc *)malloc(sizeof(Doc));
    (*pDoc)->id.buf = (char*)id;
    (*pDoc)->id.size = idlen;
    (*pDoc)->data.buf = (char*)value;
    (*pDoc)->data.size = valuelen;

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_walk_id_tree(Db *db,
                                           const sized_buf* startDocID,
                                           couchstore_docinfos_options options,
                                           couchstore_walk_tree_callback_fn callback,
                                           void *ctx)
{
    int c_ret = 0;
    leveldb_iterator_t *lit;
    leveldb_readoptions_t *read_options;
    DocInfo doc_info;
    Doc doc;
    char *keyptr, *valueptr;
    size_t valuelen;

    read_options = leveldb_readoptions_create();
    lit = leveldb_create_iterator(db->db, read_options);
    leveldb_iter_seek(lit, startDocID->buf, startDocID->size);

    while (leveldb_iter_valid(lit)) {
        keyptr = (char*)leveldb_iter_key(lit, &doc_info.id.size);
        valueptr = (char*)leveldb_iter_value(lit, &valuelen);

        doc_info.id.buf = (char *)malloc(doc_info.id.size);
        memcpy(doc_info.id.buf, keyptr, doc_info.id.size);
        doc.data.buf = (char *)malloc(valuelen);
        memcpy(doc.data.buf, valueptr, valuelen);

        c_ret = callback(db, 0, &doc_info, 0, NULL, ctx);

        free(doc_info.id.buf);
        free(doc.data.buf);

        if (c_ret != 0) {
            break;
        }

        leveldb_iter_next(lit);
    }

    leveldb_iter_destroy(lit);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
void couchstore_free_document(Doc *doc)
{
    if (doc->id.buf) free(doc->id.buf);
    if (doc->data.buf) free(doc->data.buf);
    free(doc);
}


LIBCOUCHSTORE_API
void couchstore_free_docinfo(DocInfo *docinfo)
{
    //free(docinfo->rev_meta.buf);
    free(docinfo);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_commit(Db *db)
{
    // do nothing (automatically performed at the end of each write batch)

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_compact_db_ex(Db* source, const char* target_filename,
        uint64_t flags, const couch_file_ops *ops)
{
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_compact_db(Db* source, const char* target_filename)
{
    return couchstore_compact_db_ex(source, target_filename, 0x0, NULL);
}

