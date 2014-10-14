#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#ifndef __APPLE__
#include <malloc.h>
#endif
#include <signal.h>
#include <dirent.h>

#include "couch_common.h"
#include "couch_db.h"
#include "adv_random.h"
#include "stopwatch.h"
#include "iniparser.h"

#include "arch.h"
#include "zipfian_random.h"
#include "keygen.h"

#include "memleak.h"

#define alca(type, n) ((type*)alloca(sizeof(type) * (n)))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))
#define randomize() srand((unsigned)time(NULL))

#ifdef __DEBUG
#ifndef __DEBUG_COUCHBENCH
    #undef DBG
    #undef DBGCMD
    #undef DBGSW
    #define DBG(args...)
    #define DBGCMD(command...)
    #define DBGSW(n, command...)
#endif
#endif

struct bench_info {
    uint8_t initialize; /* flag for initialization */
    uint64_t cache_size; /* buffer cache size (for fdb, rdb, ldb) */
    int auto_compaction; /* compaction mode */

    uint64_t wbs_init; /* write buffer size for bulk load */
    uint64_t wbs_bench; /* write buffer size for normal benchmark */
    uint64_t fdb_wal; /* WAL size for fdb */
    int wt_type; /* WiredTiger: B+tree or LSM-tree? */

    // # docs, # files, filename
    size_t ndocs;
    char *init_filename;
    char *filename;
    char *log_filename;
    size_t nfiles;

    // population
    size_t pop_nthreads;
    size_t pop_batchsize;
    uint8_t pop_commit;
    uint8_t fdb_flush_wal;

    // key generation (prefix)
    size_t nlevel;
    size_t nprefixes;
    struct keygen keygen;

    // benchmark threads
    size_t nreaders;
    size_t nwriters;
    size_t reader_ops;
    size_t writer_ops;

    // benchmark details
    struct rndinfo keylen;
    struct rndinfo prefixlen;
    struct rndinfo bodylen;
    size_t nbatches;
    size_t nops;
    size_t bench_secs;
    struct rndinfo batch_dist;
    struct rndinfo rbatchsize;
    struct rndinfo wbatchsize;
    struct rndinfo op_dist;
    size_t batchrange;
    uint8_t read_query_byseq;

    // percentage
    size_t write_prob;
    size_t compact_thres;
    size_t compact_period;

    // synchronous write
    uint8_t sync_write;
};

#define MIN(a,b) (((a)<(b))?(a):(b))

static uint32_t rnd_seed;
FILE *log_fp = NULL;
#define lprintf(...) {   \
    printf(__VA_ARGS__); \
    if (log_fp) fprintf(log_fp, __VA_ARGS__); } \

int _cmp_docs(const void *a, const void *b)
{
    Doc *aa, *bb;
    aa = (Doc *)a;
    bb = (Doc *)b;

    if (aa->id.size == bb->id.size) return memcmp(aa->id.buf, bb->id.buf, aa->id.size);
    else {
        size_t len = MIN(aa->id.size , bb->id.size);
        int cmp = memcmp(aa->id.buf, bb->id.buf, len);
        if (cmp != 0) return cmp;
        else {
            return (aa->id.size - bb->id.size);
        }
    }
}

static uint8_t metabuf[256];

#define PRINT_TIME(t,str) \
    printf("%d.%01d"str, (int)(t).tv_sec, (int)(t).tv_usec / 100000);
#define LOG_PRINT_TIME(t,str) \
    lprintf("%d.%01d"str, (int)(t).tv_sec, (int)(t).tv_usec / 100000);

uint64_t get_filesize(char *filename)
{
    struct stat filestat;
    stat(filename, &filestat);
    return filestat.st_size;
}

char * print_filesize_approx(uint64_t size, char *output)
{
    if (size < 1024*1024) {
        sprintf(output, "%.2f KB", (double)size / 1024);
    }else if (size >= 1024*1024 && size < 1024*1024*1024) {
        sprintf(output, "%.2f MB", (double)size / (1024*1024));
    }else {
        sprintf(output, "%.2f GB", (double)size / (1024*1024*1024));
    }
    return output;
}

void print_filesize(char *filename)
{
    char buf[256];
    uint64_t size = get_filesize(filename);

    printf("file size : %lu bytes (%s)\n",
           (unsigned long)size, print_filesize_approx(size, buf));
}

int empty_callback(Db *db, DocInfo *docinfo, void *ctx)
{
    return 0;
}

#define MAX_KEYLEN (4096)
void _create_doc(struct bench_info *binfo, size_t idx, Doc **pdoc, DocInfo **pinfo)
{
    int r;
    uint32_t crc;
    Doc *doc = *pdoc;
    DocInfo *info = *pinfo;
    char keybuf[MAX_KEYLEN];

    crc = keygen_idx2crc(idx, 0);
    BDR_RNG_VARS_SET(crc);

    if (!doc) {
        doc = (Doc *)malloc(sizeof(Doc));
        doc->id.buf = NULL;
        doc->data.buf = NULL;
    }

    doc->id.size = keygen_seed2key(&binfo->keygen, idx, keybuf);
    if (!doc->id.buf) {
        doc->id.buf = (char *)malloc(MAX_KEYLEN);
    }
    memcpy(doc->id.buf, keybuf, doc->id.size);

    BDR_RNG_NEXTPAIR;
    r = get_random(&binfo->bodylen, rngz, rngz2);
    if (r < 8) r = 8;

    doc->data.size = r;
    // align to 8 bytes (sizeof(uint64_t))
    doc->data.size = (size_t)((doc->data.size+1) / (sizeof(uint64_t)*1)) *
                     (sizeof(uint64_t)*1);
    if (!doc->data.buf) {
        int max_bodylen;
        if (binfo->bodylen.type == RND_NORMAL) {
            max_bodylen = binfo->bodylen.a + binfo->bodylen.b * 6;
        } else {
            // uniform
            max_bodylen = binfo->bodylen.b + 16;
        }
        doc->data.buf = (char *)malloc(max_bodylen);
    }
    memset(doc->data.buf, 'x', doc->data.size);
    memcpy(doc->data.buf + doc->data.size - 5, (void*)"<end>", 5);
    snprintf(doc->data.buf, doc->data.size,
             "idx# %d, body of %.*s, key len %d, body len %d",
             (int)idx, (int)doc->id.size, doc->id.buf,
             (int)doc->id.size, (int)doc->data.size);

    if (!info)
        info = (DocInfo*)malloc(sizeof(DocInfo));

    memset(info, 0, sizeof(DocInfo));
    info->id = doc->id;
    info->rev_meta.buf = (char *)metabuf;
    info->rev_meta.size = 4;

    *pdoc = doc;
    *pinfo = info;
}

struct pop_thread_args {
    int n;
    Db **db;
    struct bench_info *binfo;
    spin_t *lock;
    uint64_t *counter;
    struct stopwatch *sw;
    struct stopwatch *sw_long;
};

#define SET_DOC_RANGE(ndocs, nfiles, idx, begin, end) \
    begin = (ndocs) * ((idx)+0) / (nfiles); \
    end = (ndocs) * ((idx)+1) / (nfiles);

#define GET_FILE_NO(ndocs, nfiles, idx) \
    ((idx) / ( ((ndocs) + (nfiles-1)) / (nfiles)))

void * pop_thread(void *voidargs)
{
    int i, k, c, n;
    uint64_t counter;
    struct pop_thread_args *args = (struct pop_thread_args *)voidargs;
    struct bench_info *binfo = args->binfo;
    size_t batchsize = args->binfo->pop_batchsize;
    Db *db;
    Doc **docs;
    DocInfo **infos;

    docs = (Doc**)calloc(batchsize, sizeof(Doc*));
    infos = (DocInfo**)calloc(batchsize, sizeof(DocInfo*));
    for (i=0;i<batchsize;++i) {
        docs[i] = NULL;
        infos[i] = NULL;
        _create_doc(binfo, i, &docs[i], &infos[i]);
    }

    for (k=args->n; k<binfo->nfiles; k+=binfo->pop_nthreads) {
        db = args->db[k];
        SET_DOC_RANGE(binfo->ndocs, binfo->nfiles, k, c, n);

        while(c < n) {
            counter = 0;
            for (i=c; (i<c+batchsize && i<n); ++i){
                _create_doc(binfo, i, &docs[i-c], &infos[i-c]);
                counter++;
            }
            spin_lock(args->lock);
            *(args->counter) += counter;
            spin_unlock(args->lock);

            couchstore_save_documents(db, docs, infos, i-c, 0x0);
            if (binfo->pop_commit) {
                couchstore_commit(db);
            }
            c = i;
        }
        if (!binfo->pop_commit) {
            couchstore_commit(db);
        }

    }

    for (i=0;i<batchsize;++i){
        if (docs[i]) {
            if (docs[i]->id.buf) free(docs[i]->id.buf);
            if (docs[i]->data.buf) free(docs[i]->data.buf);
            free(docs[i]);
        }
        if (infos[i]) free(infos[i]);
    }

    free(docs);
    free(infos);
    thread_exit(0);
    return NULL;
}

void * pop_print_time(void *voidargs)
{
    uint64_t counter = 0;
    uint64_t c = 0;
    uint64_t remain_sec;
    uint64_t elapsed_ms = 0;
    struct pop_thread_args *args = (struct pop_thread_args *)voidargs;
    struct bench_info *binfo = args->binfo;
    struct timeval tv;

    while (counter < binfo->ndocs) {
        spin_lock(args->lock);
        counter = *(args->counter);
        if (stopwatch_check_ms(args->sw, 100)) {
            tv = stopwatch_stop(args->sw_long);
            stopwatch_start(args->sw);

            if (++c % 10 == 0 && counter) {
                elapsed_ms = (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
                remain_sec = (binfo->ndocs - counter);
                remain_sec = remain_sec / MAX(1, (counter / elapsed_ms));
                remain_sec = remain_sec / 1000;
            }
            printf("\r[%d.%d s] ", (int)tv.tv_sec, (int)(tv.tv_usec/100000));
            printf("%"_F64" / %"_F64, counter, (uint64_t)binfo->ndocs);
            printf(" (%d %%)", (int)(counter * 100 / binfo->ndocs));
            printf(" (-%d s)", (int)remain_sec);
            spin_unlock(args->lock);
            fflush(stdout);
        } else {
            spin_unlock(args->lock);
            usleep(100000);
        }
    }
    return NULL;
}

void population(Db **db, struct bench_info *binfo)
{
    int i;
    thread_t tid[binfo->pop_nthreads+1];
    void *ret[binfo->pop_nthreads+1];
    struct pop_thread_args args[binfo->pop_nthreads + 1];
    spin_t lock;
    uint64_t counter;
    struct stopwatch sw, sw_long;
    struct timeval tv;

    spin_init(&lock);
    stopwatch_init(&sw);
    stopwatch_start(&sw);
    stopwatch_init(&sw_long);
    stopwatch_start(&sw_long);
    counter = 0;

    for (i=0;i<=binfo->pop_nthreads;++i){
        args[i].n = i;
        args[i].db = db;
        args[i].binfo = binfo;
        args[i].lock = &lock;
        args[i].sw = &sw;
        args[i].sw_long = &sw_long;
        args[i].counter = &counter;
        if (i<binfo->pop_nthreads) {
            thread_create(&tid[i], pop_thread, &args[i]);
        } else {
            thread_create(&tid[i], pop_print_time, &args[i]);
        }
    }

    for (i=0;i<=binfo->pop_nthreads;++i){
        thread_join(tid[i], &ret[i]);
    }

    spin_destroy(&lock);

    tv = stopwatch_stop(&sw_long);
    printf("\r[%d.%d s] ", (int)tv.tv_sec, (int)(tv.tv_usec/100000));
    printf("%"_F64" / %"_F64, counter, (uint64_t)binfo->ndocs);
    printf(" (%d %%)", 100);
    printf(" (-%d s)\n", 0);
    fflush(stdout);
}

#if defined(__linux) && !defined(__ANDROID__)
    #define __PRINT_IOSTAT
#endif
uint64_t print_proc_io_stat(char *buf)
{
#ifdef __PRINT_IOSTAT
    sprintf(buf, "/proc/%d/io", getpid());
    char str[64];
    int ret;
    unsigned long temp;
    uint64_t val=0;
    FILE *fp = fopen(buf, "r");
    while(!feof(fp)) {
        ret = fscanf(fp, "%s %lu", str, &temp);
        if (!strcmp(str, "write_bytes:")) {
            val = temp;
            lprintf("[proc IO] %lu bytes written (%s)\n",
                    (unsigned long)val, print_filesize_approx(val, str));
        }
    }
    fclose(fp);
    return val;

#else
    return 0;
#endif
}

void _get_rw_factor(struct bench_info *binfo, double *prob)
{
    double p = binfo->write_prob / 100.0;
    double wb, rb;

    // rw_factor = r_batchsize / w_batchsize
    if (binfo->rbatchsize.type == RND_NORMAL) {
        rb = (double)binfo->rbatchsize.a;
        wb = (double)binfo->wbatchsize.a;
    }else{
        rb = (double)(binfo->rbatchsize.a + binfo->rbatchsize.b) / 2;
        wb = (double)(binfo->wbatchsize.a + binfo->wbatchsize.b) / 2;
    }
    if (binfo->write_prob < 100) {
        *prob = (p * rb) / ( (1-p)*wb + p*rb );
    }else{
        *prob = 65536;
    }
}

struct bench_result_hit{
    uint32_t idx;
    uint64_t hit;
};

struct bench_result{
    struct bench_info *binfo;
    uint64_t ndocs;
    uint64_t nfiles;
    struct bench_result_hit *doc_hit;
    struct bench_result_hit *file_hit;
};

//#define __BENCH_RESULT
#ifdef __BENCH_RESULT
void _bench_result_init(struct bench_result *result, struct bench_info *binfo)
{
    size_t i;

    result->binfo = binfo;
    result->ndocs = binfo->ndocs;
    result->nfiles = binfo->nfiles;

    result->doc_hit = (struct bench_result_hit*)
                      malloc(sizeof(struct bench_result_hit) * binfo->ndocs);
    result->file_hit = (struct bench_result_hit*)
                       malloc(sizeof(struct bench_result_hit) * binfo->nfiles);
    memset(result->doc_hit, 0, sizeof(struct bench_result_hit) * binfo->ndocs);
    memset(result->file_hit, 0, sizeof(struct bench_result_hit) * binfo->nfiles);

    for (i=0;i<binfo->ndocs;++i){
        result->doc_hit[i].idx = i;
    }
    for (i=0;i<binfo->nfiles;++i){
        result->file_hit[i].idx = i;
    }
}

void _bench_result_doc_hit(struct bench_result *result, uint64_t idx)
{
    result->doc_hit[idx].hit++;
}

void _bench_result_file_hit(struct bench_result *result, uint64_t idx)
{
    result->file_hit[idx].hit++;
}

int _bench_result_cmp(const void *a, const void *b)
{
    struct bench_result_hit *aa, *bb;
    aa = (struct bench_result_hit*)a;
    bb = (struct bench_result_hit*)b;
    // decending (reversed) order
    if (aa->hit < bb->hit) return 1;
    else if (aa->hit > bb->hit) return -1;
    else return 0;
}

void _bench_result_print(struct bench_result *result)
{
    char buf[1024];
    size_t i, keylen;
    uint32_t crc;
    uint64_t file_sum, doc_sum, cum;
    FILE *fp;

    file_sum = doc_sum = 0;

    printf("printing bench results.. \n");

    for (i=0;i<result->nfiles;++i) file_sum += result->file_hit[i].hit;
    for (i=0;i<result->ndocs;++i) doc_sum += result->doc_hit[i].hit;

    qsort(result->file_hit, result->nfiles,
         sizeof(struct bench_result_hit), _bench_result_cmp);
    qsort(result->doc_hit, result->ndocs,
         sizeof(struct bench_result_hit), _bench_result_cmp);

    fp = fopen("result.txt", "w");
    fprintf(fp, "== files ==\n");
    cum = 0;
    for (i=0;i<result->nfiles;++i){
        cum += result->file_hit[i].hit;

        fprintf(fp, "%8d%8d%8.1f %%%8.1f %%\n",
                (int)result->file_hit[i].idx,
                (int)result->file_hit[i].hit,
                100.0 * result->file_hit[i].hit/ file_sum,
                100.0 * cum / file_sum);
    }

    fprintf(fp, "== documents ==\n");
    cum = 0;
    for (i=0;i<result->ndocs;++i){
        cum += result->doc_hit[i].hit;
        if (result->doc_hit[i].hit > 0) {
            fprintf(fp, "%8d%8d%8.1f %%%8.1f %%\n",
                    (int)result->doc_hit[i].idx,
                    (int)result->doc_hit[i].hit,
                    100.0 * result->doc_hit[i].hit/ doc_sum,
                    100.0 * cum / doc_sum);
        }
    }
    fclose(fp);
}

void _bench_result_free(struct bench_result *result)
{
    free(result->doc_hit);
    free(result->file_hit);
}

#else
#define _bench_result_init(a,b)
#define _bench_result_doc_hit(a,b)
#define _bench_result_file_hit(a,b)
#define _bench_result_cmp(a,b)
#define _bench_result_print(a)
#define _bench_result_free(a)
#endif

#define OP_CLOSE (0x01)
#define OP_CLOSE_OK (0x02)
#define OP_REOPEN (0x04)
struct bench_thread_args {
    int id;
    Db **db;
    int mode; // 0:reader+writer, 1:writer, 2:reader
    int *compaction_no;
    uint32_t rnd_seed;
    struct bench_info *binfo;
    struct bench_result *result;
    struct zipf_rnd *zipf;
    struct bench_shared_stat *b_stat;
    uint8_t terminate_signal;
    uint8_t op_signal;
};

struct compactor_args {
    char *curfile;
    char *newfile;
    struct bench_info *binfo;
    struct stopwatch *sw_compaction;
    struct bench_thread_args *b_args;
    int *cur_compaction;
    int bench_threads;
    uint8_t flag;
    spin_t *lock;
};

#if defined(__FDB_BENCH) || defined(__COUCH_BENCH)

void * compactor(void *voidargs)
{
    struct compactor_args *args = (struct compactor_args*)voidargs;
    struct stopwatch *sw_compaction = args->sw_compaction;
    Db *db;
    char *curfile = args->curfile;
    char *newfile = args->newfile;
    uint64_t ndocs_prev;

    couchstore_open_db(curfile,
                       COUCHSTORE_OPEN_FLAG_CREATE |
                           ((args->binfo->sync_write)?(0x10):(0x0)),
                       &db);

    stopwatch_start(sw_compaction);
    couchstore_compact_db(db, newfile);
    couchstore_close_db(db);
    stopwatch_stop(sw_compaction);

    spin_lock(args->lock);
    *(args->cur_compaction) = -1;
    if (args->flag & 0x1) {
        int ret, i;
        char cmd[256];
        // erase previous db file
        sprintf(cmd, "rm -rf %s 2> errorlog.txt", curfile);
        ret = system(cmd);

        for (i=0; i<args->bench_threads; ++i) {
            args->b_args[i].op_signal |= OP_REOPEN;
        }
    }
    spin_unlock(args->lock);

    return NULL;
}

#endif

void (*old_handler)(int);
int got_signal = 0;
void signal_handler_confirm(int sig_no)
{
    char *r;
    char cmd[1024];
    printf("\nAre you sure to terminate (y/N)? ");
    r = fgets(cmd, 1024, stdin);
    if (r == cmd && (cmd[0] == 'y' || cmd[0] == 'Y')) {
        printf("Force benchmark program to terminate.. (data loss may occur)\n");
        exit(0);
    }
}
void signal_handler(int sig_no)
{
    printf("\n ### got Ctrl+C ### \n");
    fflush(stdout);
    got_signal = 1;

    // set signal handler
    signal(SIGINT, signal_handler_confirm);
}

void _dir_scan(struct bench_info *binfo, int *compaction_no)
{
    int filename_len = strlen(binfo->filename);
    int dirname_len = 0;
    int i;
    int db_no, cpt_no;
    char dirname[256], *filename;
    DIR *dir_info;
    struct dirent *dir_entry;

    if (binfo->filename[filename_len-1] == '/') {
        filename_len--;
    }
    // backward search until find first '/'
    for (i=filename_len-1; i>=0; --i){
        if (binfo->filename[i] == '/') {
            dirname_len = i+1;
            break;
        }
    }
    if (dirname_len > 0) {
        strncpy(dirname, binfo->filename, dirname_len);
        dirname[dirname_len] = 0;
    } else {
        strcpy(dirname, ".");
    }
    filename = binfo->filename + dirname_len;

    dir_info = opendir(dirname);
    if (dir_info != NULL) {
        while((dir_entry = readdir(dir_info))) {
            if (!strncmp(dir_entry->d_name, filename, strlen(filename))) {
                sscanf(dir_entry->d_name + (filename_len - dirname_len),
                       "%d.%d", &db_no, &cpt_no);
                compaction_no[db_no] = cpt_no;
            }
        }
    }
}

struct bench_shared_stat {
    uint64_t op_count_read;
    uint64_t op_count_write;
    uint64_t batch_count;
    spin_t lock;
};

void * bench_thread(void *voidargs)
{
    struct bench_thread_args *args = (struct bench_thread_args *)voidargs;
    int i, j;
    int batchsize;
    int write_mode, write_mode_r;
    int commit_mask[args->binfo->nfiles];
    int curfile_no, file_doccount[args->binfo->nfiles], c;
    double prob, ratio;
    char curfile[256], keybuf[MAX_KEYLEN];
    uint64_t r, crc, op_med;
    uint64_t op_w, op_r, op_w_cum, op_r_cum, op_w_turn, op_r_turn;
    uint64_t expected_us, elapsed_us, elapsed_sec;
    Db **db;
    Doc *rq_doc, **rq_doc_arr[args->binfo->nfiles];
    DocInfo *rq_info, **rq_info_arr[args->binfo->nfiles];
    sized_buf rq_id;
    struct rndinfo write_mode_random, op_dist;
    struct bench_info *binfo = args->binfo;
    struct bench_result *result = args->result;
    struct zipf_rnd *zipf = args->zipf;
    struct stopwatch sw;
    struct timeval gap;
    couchstore_error_t err;

    // uint64_t *offset_arr = (uint64_t*)malloc(sizeof(uint64_t) * args->binfo->ndocs);

    db = args->db;

    op_med = op_w = op_r = op_w_cum = op_r_cum = 0;
    elapsed_us = 0;
    write_mode_random.type = RND_UNIFORM;
    write_mode_random.a = 0;
    write_mode_random.b = 256 * 256;

    crc = args->rnd_seed;
    crc = MurmurHash64A(&crc, sizeof(crc), 0);
    BDR_RNG_VARS_SET(crc);
    BDR_RNG_NEXTPAIR;
    BDR_RNG_NEXTPAIR;
    BDR_RNG_NEXTPAIR;

    stopwatch_init_start(&sw);

    // calculate rw_factor and write probability
    _get_rw_factor(binfo, &prob);

    while(!args->terminate_signal) {
        if (args->op_signal & OP_CLOSE) {
            args->op_signal |= OP_CLOSE_OK; // set ack flag
            args->op_signal &= ~(OP_CLOSE); // clear flag
            while (!(args->op_signal & OP_REOPEN)) {
                usleep(10000); // couchstore cannot write during compaction
            }
            if (args->op_signal & OP_CLOSE) {
                // compaction again
                continue;
            }
        }
        if (args->op_signal & OP_REOPEN) {
            for (i=0;i<args->binfo->nfiles;++i) {
                couchstore_close_db(args->db[i]);
                sprintf(curfile, "%s%d.%d", binfo->filename, i, args->compaction_no[i]);
                couchstore_open_db(curfile, 0x0, &args->db[i]);
            }
            args->op_signal = 0;
        }
        gap = stopwatch_get_curtime(&sw);
        elapsed_us = _timeval_to_us(gap);
        if (elapsed_us == 0) elapsed_us = 1;
        elapsed_sec = elapsed_us / 1000000;

        spin_lock(&args->b_stat->lock);
        op_w = args->b_stat->op_count_write;
        op_r = args->b_stat->op_count_read;
        spin_unlock(&args->b_stat->lock);

        BDR_RNG_NEXTPAIR;
        switch(args->mode) {
        case 0: // reader+writer
            // decide write or read
            write_mode_r = get_random(&write_mode_random, rngz, rngz2);
            write_mode = ( (prob * 65536.0) > write_mode_r);
            break;

        case 1: // writer
            write_mode = 1;
            if (binfo->writer_ops > 0 && binfo->write_prob > 100) {
                // ops mode
                if (op_w_cum < elapsed_sec * binfo->writer_ops) break;
                op_w_turn = op_w_cum - elapsed_sec*binfo->writer_ops;
                if (op_w_turn < binfo->writer_ops) {
                    expected_us = 1000000 * op_w_turn / binfo->writer_ops;
                } else {
                    expected_us = 1000000;
                }
                expected_us += elapsed_sec * 1000000;
                if (expected_us > elapsed_us) {
                    usleep(expected_us - elapsed_us);
                }
            } else {
                if (op_w * 100 > (op_w + op_r) * binfo->write_prob &&
                    binfo->write_prob <= 100) {
                    usleep(1);
                    continue;
                }
            }
            break;

        case 2: // reader
            write_mode = 0;
            if (binfo->reader_ops > 0 && binfo->write_prob > 100) {
                // ops mode
                if (op_r_cum < elapsed_sec * binfo->reader_ops) break;
                op_r_turn = op_r_cum - elapsed_sec*binfo->reader_ops;
                if (op_r_turn < binfo->reader_ops) {
                    expected_us = 1000000 * op_r_turn / binfo->reader_ops;
                } else {
                    expected_us = 1000000;
                }
                expected_us += elapsed_sec * 1000000;
                if (expected_us > elapsed_us) {
                    usleep(expected_us - elapsed_us);
                }
            } else {
                if (op_w * 100 < (op_w + op_r) * binfo->write_prob &&
                    binfo->write_prob <= 100) {
                    usleep(1);
                    continue;
                }
            }
            break;
        }

        // randomly set batchsize
        BDR_RNG_NEXTPAIR;
        if (write_mode) {
            batchsize = get_random(&binfo->wbatchsize, rngz, rngz2);
            if (batchsize <= 0) batchsize = 1;
        }else{
            batchsize = get_random(&binfo->rbatchsize, rngz, rngz2);
            if (batchsize <= 0) batchsize = 1;
        }

        // ramdomly set document distribution for batch
        if (binfo->batch_dist.type == RND_UNIFORM) {
            // uniform distribution
            BDR_RNG_NEXTPAIR;
            op_med = get_random(&binfo->batch_dist, rngz, rngz2);
        }else{
            // zipfian distribution
            BDR_RNG_NEXTPAIR;
            op_med = zipf_rnd_get(zipf);
            op_med = op_med * binfo->batch_dist.b + (rngz % binfo->batch_dist.b);
        }
        if (op_med >= binfo->ndocs) op_med = binfo->ndocs - 1;

        // distribution of operations in a batch
        if (binfo->op_dist.type == RND_NORMAL){
            op_dist.type = RND_NORMAL;
            op_dist.a = op_med;
            op_dist.b = binfo->batchrange/2;
        }else {
            op_dist.type = RND_UNIFORM;
            op_dist.a = op_med - binfo->batchrange;
            op_dist.b = op_med + binfo->batchrange;
            if (op_dist.a < 0) op_dist.a = 0;
            if (op_dist.b >= binfo->ndocs) op_dist.b = binfo->ndocs;
        }

        if (write_mode) {
            // write (update)
#if defined(__FDB_BENCH) || defined(__WT_BENCH)
            // initialize
            memset(commit_mask, 0, sizeof(int) * binfo->nfiles);

            for (j=0;j<batchsize;++j){
                rq_doc = NULL;
                rq_info = NULL;

                BDR_RNG_NEXTPAIR;
                r = get_random(&op_dist, rngz, rngz2);
                if (r >= binfo->ndocs) r = r % binfo->ndocs;
                curfile_no = GET_FILE_NO(binfo->ndocs, binfo->nfiles, r);
                _bench_result_doc_hit(result, r);
                _bench_result_file_hit(result, curfile_no);
                //printf("%22"_X64" %22"_X64" %6d %6d\n", rngz, rngz2, op_med, (int)r);

                _create_doc(binfo, r, &rq_doc, &rq_info);
                err = couchstore_save_document(db[curfile_no], rq_doc, rq_info, 0x0);

                // set mask
                commit_mask[curfile_no] = 1;

                free(rq_doc->id.buf);
                free(rq_doc->data.buf);
                free(rq_doc);
                free(rq_info);
            }

            for (j=0;j<binfo->nfiles;++j) {
                if (commit_mask[j]) {
                    couchstore_commit(db[j]);
                }
            }
#else
            for (i=0; i<binfo->nfiles;++i){
                rq_doc_arr[i] = (Doc **)malloc(sizeof(Doc*) * batchsize);
                rq_info_arr[i] = (DocInfo **)malloc(sizeof(DocInfo*) * batchsize);
                memset(rq_doc_arr[i], 0, sizeof(Doc*) * batchsize);
                memset(rq_info_arr[i], 0, sizeof(DocInfo*) * batchsize);
                file_doccount[i] = 0;
            }

            for (j=0;j<batchsize;++j){
                BDR_RNG_NEXTPAIR;
                r = get_random(&op_dist, rngz, rngz2);
                if (r >= binfo->ndocs) r = r % binfo->ndocs;
                curfile_no = GET_FILE_NO(binfo->ndocs, binfo->nfiles, r);
                _bench_result_doc_hit(result, r);
                _bench_result_file_hit(result, curfile_no);

                c = file_doccount[curfile_no]++;
                _create_doc(binfo, r,
                            &rq_doc_arr[curfile_no][c],
                            &rq_info_arr[curfile_no][c]);

                // set mask
                commit_mask[curfile_no] = 1;

            }

            for (i=0;i<binfo->nfiles;++i) {
                if (file_doccount[i] > 0) {
                    err = couchstore_save_documents(db[curfile_no],
                                                    rq_doc_arr[i],
                                                    rq_info_arr[i],
                                                    file_doccount[i], 0x0);
#if defined(__COUCH_BENCH)
                    err = couchstore_commit(db[curfile_no]);
#endif
                    for (j=0;j<file_doccount[i];++j){
                        free(rq_doc_arr[i][j]->id.buf);
                        free(rq_doc_arr[i][j]->data.buf);
                        free(rq_doc_arr[i][j]);
                        free(rq_info_arr[i][j]);
                    }
                }
                free(rq_doc_arr[i]);
                free(rq_info_arr[i]);
            }
#endif

            spin_lock(&args->b_stat->lock);
            args->b_stat->op_count_write += batchsize;
            args->b_stat->batch_count++;
            spin_unlock(&args->b_stat->lock);

            op_w_cum += batchsize;
        }else{
            // read
            for (j=0;j<batchsize;++j){

                BDR_RNG_NEXTPAIR;
                r = get_random(&op_dist, rngz, rngz2);
                if (r >= binfo->ndocs) r = r % binfo->ndocs;
                curfile_no = GET_FILE_NO(binfo->ndocs, binfo->nfiles, r);
                _bench_result_doc_hit(result, r);
                _bench_result_file_hit(result, curfile_no);

                rq_id.size = keygen_seed2key(&binfo->keygen, r, keybuf);
                rq_id.buf = (char *)malloc(rq_id.size);
                memcpy(rq_id.buf, keybuf, rq_id.size);

                err = couchstore_open_document(db[curfile_no], rq_id.buf,
                                               rq_id.size, &rq_doc, 0x0);
                if (err != COUCHSTORE_SUCCESS) {
                    printf("read error: document number %"_F64"\n", r);
                }

                rq_doc->id.buf = NULL;
                couchstore_free_document(rq_doc);
                free(rq_id.buf);
            }

            spin_lock(&args->b_stat->lock);
            args->b_stat->op_count_read += batchsize;
            args->b_stat->batch_count++;
            spin_unlock(&args->b_stat->lock);

            op_r_cum += batchsize;
        }
    }

    return NULL;
}

void _wait_leveldb_compaction(struct bench_info *binfo, Db **db)
{
    int n=6;
    int i, ret;
    char buf[256], str[64];
    unsigned long temp;
    uint64_t count=0;
    uint64_t val[n];
    FILE *fp;

    for (i=0;i<n;++i){
        val[i] = i; // set different value
    }

    sprintf(buf, "/proc/%d/io", getpid());

    lprintf("waiting for background compaction of LevelDB (RocksDB) log files..");
    fflush(stdout);

    while(1) {
        fp = fopen(buf, "r");
        while(!feof(fp)) {
            ret = fscanf(fp, "%s %lu", str, &temp);
            if (!strcmp(str, "write_bytes:")) {
                val[count] = temp;
                count = (count+1) % n;
                for (i=1;i<n;++i){
                    if (val[i-1] != val[i]) goto wait_next;
                }
                lprintf(" done\n");
                fflush(stdout);
                fclose(fp);
                return;
            }
        }

wait_next:
        fclose(fp);
        usleep(600000);
    }
}

// non-standard functions for extension
couchstore_error_t couchstore_set_flags(uint64_t flags);
couchstore_error_t couchstore_set_cache(uint64_t size);
couchstore_error_t couchstore_set_compaction(int mode,
                                             size_t threshold);
couchstore_error_t couchstore_set_chk_period(size_t seconds);
couchstore_error_t couchstore_open_conn(const char *filename);
couchstore_error_t couchstore_close_conn();
couchstore_error_t couchstore_set_wal_size(size_t size);
couchstore_error_t couchstore_set_wbs_size(uint64_t size);
couchstore_error_t couchstore_set_idx_type(int type);
couchstore_error_t couchstore_set_sync(Db *db, int sync);

int _does_file_exist(char *filename) {
    struct stat st;
    int result = stat(filename, &st);
    return result == 0;
}

char *_get_dirname(char *filename, char *dirname_buf)
{
    int i;
    int len = strlen(filename);

    // find first '/' from right
    for (i=len-1; i>=0; --i){
        if (filename[i] == '/' && i>0) {
        	memcpy(dirname_buf, filename, i);
        	dirname_buf[i] = 0;
        	return dirname_buf;
        }
    }
    return NULL;
}

void do_bench(struct bench_info *binfo)
{
    BDR_RNG_VARS;
    int i, j, ret;
    int curfile_no, compaction_turn;
    int op_count_read, op_count_write;
    int prev_op_count_read, prev_op_count_write;
    int compaction_no[binfo->nfiles], total_compaction = 0;
    int cur_compaction = -1;
    int bench_threads;
    uint64_t written_init, written_final;
    char curfile[256], newfile[256], bodybuf[1024], cmd[256];
    char fsize1[128], fsize2[128], *str;
    void *compactor_ret;
    void **bench_worker_ret;
    double gap_double;
    Db *db[binfo->nfiles], *info_handle[binfo->nfiles];
    DbInfo *dbinfo;
    thread_t tid_compactor;
    thread_t *bench_worker;
    spin_t cur_compaction_lock;
    struct stopwatch sw, sw_compaction, progress;
    struct timeval gap, _gap;
    struct zipf_rnd zipf;
    struct bench_result result;
    struct compactor_args c_args;
    struct bench_shared_stat b_stat;
    struct bench_thread_args *b_args;

    memleak_start();

    spin_init(&cur_compaction_lock);

    dbinfo = (DbInfo *)malloc(sizeof(DbInfo));
    stopwatch_init(&sw);
    stopwatch_init(&sw_compaction);

    _bench_result_init(&result, binfo);

    written_init = written_final = 0;

#if !defined(__COUCH_BENCH)
    couchstore_set_cache(binfo->cache_size);
#endif
#if defined(__FDB_BENCH)
    couchstore_set_compaction(binfo->auto_compaction, binfo->compact_thres);
    couchstore_set_wal_size(binfo->fdb_wal);
#endif

    if (binfo->initialize) {
        // === initialize and populate files ========
        // erase previous db file
        lprintf("\ninitialize\n");
        sprintf(cmd, "rm -rf %s* 2> errorlog.txt", binfo->filename);
        ret = system(cmd);

        // create directory if doesn't exist
        str = _get_dirname(binfo->filename, bodybuf);
        if (str) {
            if (!_does_file_exist(str)) {
                sprintf(cmd, "mkdir -p %s > errorlog.txt", str);
                ret = system(cmd);
            }
        }

#if defined(__WT_BENCH)
        // WiredTiger: open connection
        couchstore_set_idx_type(binfo->wt_type);
        couchstore_open_conn((char*)binfo->filename);
#endif
#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
        // LevelDB, RocksDB: set WBS size
        couchstore_set_wbs_size(binfo->wbs_init);
#endif

        for (i=0;i<binfo->nfiles;++i){
            compaction_no[i] = 0;
            sprintf(curfile, "%s%d.%d", binfo->init_filename, i, compaction_no[i]);
#if defined(__FDB_BENCH)
            if (!binfo->pop_commit) {
                // set wal_flush_before_commit flag (0x1)
                couchstore_set_flags(0x1);
            }
#endif
            couchstore_open_db(curfile, COUCHSTORE_OPEN_FLAG_CREATE, &db[i]);
#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
            if (!binfo->pop_commit) {
                couchstore_set_sync(db[i], 0);
            }
#endif
        }

        stopwatch_start(&sw);
        population(db, binfo);

#ifdef __PRINT_IOSTAT
#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
        gap = stopwatch_stop(&sw);
        LOG_PRINT_TIME(gap, " sec elapsed\n");
        print_proc_io_stat(cmd);
        _wait_leveldb_compaction(binfo, db);
#endif // __LEVEL_BENCH || __ROCKS_BENCH
#endif // __PRINT_IOSTAT
        if (binfo->sync_write) {
            lprintf("flushing disk buffer.. "); fflush(stdout);
            sprintf(cmd, "sync");
            ret = system(cmd);
            lprintf("done\n"); fflush(stdout);
        }

        written_init = print_proc_io_stat(cmd);

        for (i=0;i<binfo->nfiles;++i){
            couchstore_close_db(db[i]);
        }
        gap = stopwatch_stop(&sw);
#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
#if defined(__PRINT_IOSTAT)
        gap.tv_sec -= 3; // subtract waiting time
#endif // __PRINT_IOSTAT
#endif // __LEVEL_BENCH || __ROCKS_BENCH
        gap_double = gap.tv_sec + (double)gap.tv_usec / 1000000.0;
        LOG_PRINT_TIME(gap, " sec elapsed ");
        lprintf("(%.2f ops/sec)\n", binfo->ndocs / gap_double);

    } else {
        // === load existing files =========
        stopwatch_start(&sw);
        for (i=0;i<binfo->nfiles;++i) {
            compaction_no[i] = 0;
        }

#if defined(__WT_BENCH)
        // for WiredTiger: open connection
        couchstore_open_conn((char*)binfo->filename);
#else
        _dir_scan(binfo, compaction_no);
#endif
    }

    // ==== perform benchmark ====
    lprintf("\nbenchmark\n");

    compaction_turn = 0;

#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
    // LevelDB, RocksDB: reset write buffer size
    couchstore_set_wbs_size(binfo->wbs_bench);
#endif
#if defined(__FDB_BENCH)
    // ForestDB: clear wal_flush_before_commit flag
    couchstore_set_flags(0x0);
#endif

    if (binfo->batch_dist.type == RND_ZIPFIAN) {
        // zipfian distribution .. initialize zipf_rnd
        zipf_rnd_init(&zipf, binfo->ndocs / binfo->batch_dist.b,
                      binfo->batch_dist.a/100.0, 1024*1024);
    }

    // set signal handler
    old_handler = signal(SIGINT, signal_handler);

    // bench stat init
    b_stat.batch_count = 0;
    b_stat.op_count_read = b_stat.op_count_write = 0;
    prev_op_count_read = prev_op_count_write = 0;
    spin_init(&b_stat.lock);

    // thread args
    if (binfo->nreaders == 0 && binfo->nwriters == 0){
        // create a rw thread
        bench_threads = 1;
        b_args = alca(struct bench_thread_args, bench_threads);
        bench_worker = alca(thread_t, bench_threads);
        b_args[0].mode = 0;
    } else {
        bench_threads = binfo->nreaders + binfo->nwriters;
        b_args = alca(struct bench_thread_args, bench_threads);
        bench_worker = alca(thread_t, bench_threads);
        for (i=0;i<bench_threads;++i){
            b_args[i].mode = (i < binfo->nwriters)?(1):(2);
        }
    }
    bench_worker_ret = alca(void*, bench_threads);
    for (i=0;i<bench_threads;++i){
        b_args[i].id = i;
        b_args[i].rnd_seed = rnd_seed;
        b_args[i].compaction_no = compaction_no;
        b_args[i].b_stat = &b_stat;
        b_args[i].result = &result;
        b_args[i].zipf = &zipf;
        b_args[i].terminate_signal = 0;
        b_args[i].op_signal = 0;
        b_args[i].binfo = binfo;

        // open db instances
#if defined(__FDB_BENCH) || defined(__COUCH_BENCH) || defined(__WT_BENCH)
        b_args[i].db = (Db**)malloc(sizeof(Db*) * binfo->nfiles);
        for (j=0;j<binfo->nfiles;++j){
            sprintf(curfile, "%s%d.%d", binfo->filename, j, compaction_no[j]);
            couchstore_open_db(curfile,
                               COUCHSTORE_OPEN_FLAG_CREATE |
                                   ((binfo->sync_write)?(0x10):(0x0)),
                               &b_args[i].db[j]);
#if defined(__FDB_BENCH)
            // ForestDB: open another handle to get DB info
            if (i==0) {
                couchstore_open_db(curfile,
                                   COUCHSTORE_OPEN_FLAG_CREATE |
                                       ((binfo->sync_write)?(0x10):(0x0)),
                                   &info_handle[j]);
            }
#endif
        }
#elif defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
        if (i==0) {
            // open only once (multiple open is not allowed)
            b_args[i].db = (Db**)malloc(sizeof(Db*) * binfo->nfiles);
            for (j=0;j<binfo->nfiles;++j){
                sprintf(curfile, "%s%d.%d", binfo->filename, j, compaction_no[j]);
                couchstore_open_db(curfile, COUCHSTORE_OPEN_FLAG_CREATE,
                                   &b_args[i].db[j]);
                couchstore_set_sync(b_args[i].db[j], binfo->sync_write);
            }
        } else {
            b_args[i].db = b_args[0].db;
        }
#endif
        thread_create(&bench_worker[i], bench_thread, (void*)&b_args[i]);
    }

    gap = stopwatch_stop(&sw);
    LOG_PRINT_TIME(gap, " sec elapsed\n");

    // timer for total elapsed time
    stopwatch_init(&sw);
    stopwatch_start(&sw);

    // timer for periodic stdout print
    stopwatch_init(&progress);
    stopwatch_start(&progress);

    i = 0;
    while (i<binfo->nbatches || binfo->nbatches == 0) {
        spin_lock(&b_stat.lock);
        op_count_read = b_stat.op_count_read;
        op_count_write = b_stat.op_count_write;
        i = b_stat.batch_count;
        spin_unlock(&b_stat.lock);

        _gap = stopwatch_stop(&progress);

        if (progress.elapsed.tv_sec * 10 + progress.elapsed.tv_usec / 100000 > 0) {
            // for every 0.1 sec, print current status
            uint64_t cur_size;
            int cpt_no;
            Db *temp_db;
            couchstore_error_t err;

            // reset stopwatch for the next period
            stopwatch_init(&progress);

            BDR_RNG_NEXTPAIR;
            spin_lock(&cur_compaction_lock);
            curfile_no = compaction_turn;
            compaction_turn = (compaction_turn + 1) % binfo->nfiles;
#ifdef __FDB_BENCH
            temp_db = info_handle[curfile_no];
#else
            temp_db = b_args[0].db[curfile_no];
#endif
            cpt_no = compaction_no[curfile_no] - ((curfile_no == cur_compaction)?(1):(0));
            spin_unlock(&cur_compaction_lock);

            couchstore_db_info(temp_db, dbinfo);
            if (binfo->auto_compaction) {
                // auto compaction
                strcpy(curfile, dbinfo->filename);
            } else {
                // manual compaction
                sprintf(curfile, "%s%d.%d",
                    binfo->filename, (int)curfile_no, cpt_no);
            }
            cur_size = get_filesize(curfile);

            stopwatch_stop(&sw);
            printf("\r");

            if (binfo->nbatches > 0) {
                printf("%5.1f %% (", i*100.0 / (binfo->nbatches-1));
                gap = sw.elapsed;
                PRINT_TIME(gap, " s, ");
            }else if (binfo->bench_secs > 0){
                printf("(");
                gap = sw.elapsed;
                PRINT_TIME(gap, " s / ");
                printf("%d s, ", (int)binfo->bench_secs);
            }else {
                printf("%5.1f %% (",
                       (op_count_read+op_count_write)*100.0 / (binfo->nops-1));
                gap = sw.elapsed;
                PRINT_TIME(gap, " s, ");
            }
            printf("%8.2f ops, ",
                (double)(op_count_read + op_count_write) /
                (gap.tv_sec + (double)gap.tv_usec / 1000000.0));
            printf("%8.2f ops)",
                (double)((op_count_read + op_count_write) -
                    (prev_op_count_read + prev_op_count_write)) /
                (_gap.tv_sec + (double)_gap.tv_usec / 1000000.0));

            if (log_fp) {
                fprintf(log_fp,
                        "%d.%01d %.2f %.2f "
                        "%d %d\n",
                        (int)gap.tv_sec, (int)gap.tv_usec / 100000,
                        (double)(op_count_read + op_count_write) /
                                (gap.tv_sec + (double)gap.tv_usec / 1000000.0),
                        (double)((op_count_read + op_count_write) -
                                (prev_op_count_read + prev_op_count_write)) /
                                (_gap.tv_sec + (double)_gap.tv_usec / 1000000.0),
                        op_count_read, op_count_write);
            }

            prev_op_count_read = op_count_read;
            prev_op_count_write = op_count_write;

            if (binfo->auto_compaction) { // auto
                print_filesize_approx(cur_size, fsize1);
            } else { // manual
                char curfile_temp[256];
                uint64_t cur_size_temp;
                sprintf(curfile_temp, "%s%d.%d",
                    binfo->filename, (int)curfile_no,
                    compaction_no[curfile_no]);
                cur_size_temp = get_filesize(curfile_temp);
                print_filesize_approx(cur_size_temp, fsize1);
            }

            print_filesize_approx(dbinfo->space_used, fsize2);
#if !defined(__WT_BENCH)
            printf(" (%s / %s)", fsize1, fsize2);
#endif
            fflush(stdout);

            stopwatch_start(&sw);

            // valid:invalid size check
            spin_lock(&cur_compaction_lock);
            if (cur_compaction == -1) {
                if (!binfo->auto_compaction &&
                    cur_size > dbinfo->space_used &&
                    binfo->compact_thres > 0 &&
                    ((cur_size - dbinfo->space_used) >
                        ((double)binfo->compact_thres/100.0)*(double)cur_size) ) {

                    // compaction
                    cur_compaction = curfile_no;
                    spin_unlock(&cur_compaction_lock);

                    total_compaction++;
                    compaction_no[curfile_no]++;

                    sprintf(curfile, "%s%d.%d",
                            binfo->filename, (int)curfile_no,
                            compaction_no[curfile_no]-1);
                    sprintf(newfile, "%s%d.%d",
                            binfo->filename, (int)curfile_no,
                            compaction_no[curfile_no]);
                    printf(" [C#%d %s >> %s]", total_compaction, curfile, newfile);
                    if (log_fp) {
                        fprintf(log_fp, " [C#%d %s >> %s]\n",
                                total_compaction, curfile, newfile);
                    }
                    fflush(stdout);

#ifdef __COUCH_BENCH
                    int signal_count = 0;
                    int bench_nrs = 0;

                    for (j=0; j<bench_threads; ++j) {
                        if (b_args[j].mode != 2) {
                            // close all non-readers
                            bench_nrs++;
                            b_args[j].op_signal |= OP_CLOSE;
                        }
                    }
                    while (signal_count < bench_nrs) {
                        signal_count = 0;
                        usleep(10000);
                        for (j=0;j<bench_threads;++j){
                            if (b_args[j].op_signal & OP_CLOSE_OK) {
                                signal_count++;
                            }
                        }
                    }

                    c_args.flag = 1;
                    c_args.binfo = binfo;
                    c_args.curfile = curfile;
                    c_args.newfile = newfile;
                    c_args.sw_compaction = &sw_compaction;
                    c_args.cur_compaction = &cur_compaction;
                    c_args.bench_threads = bench_threads;
                    c_args.b_args = b_args;
                    c_args.lock = &cur_compaction_lock;
                    thread_create(&tid_compactor, compactor, &c_args);
#endif
#ifdef __FDB_BENCH
                    c_args.binfo = binfo;
                    c_args.curfile = curfile;
                    c_args.newfile = newfile;
                    c_args.sw_compaction = &sw_compaction;
                    c_args.cur_compaction = &cur_compaction;
                    c_args.bench_threads = bench_threads;
                    c_args.b_args = b_args;
                    c_args.lock = &cur_compaction_lock;
                    c_args.flag = 0;
                    thread_create(&tid_compactor, compactor, &c_args);
#endif
                } else {
                    spin_unlock(&cur_compaction_lock);
                }
            } else {
                spin_unlock(&cur_compaction_lock);
            }

            if (sw.elapsed.tv_sec >= binfo->bench_secs &&
                binfo->bench_secs > 0) break;

            stopwatch_start(&progress);
        } else {
            // sleep 0.1 sec
            stopwatch_start(&progress);
            usleep(100000);
        }

next_loop:
        if ((op_count_read + op_count_write) >= binfo->nops &&
            binfo->nops > 0) break;

        if (got_signal) {
            break;
        }
    }

    // terminate all bench_worker threads
    for (i=0;i<bench_threads;++i){
        b_args[i].terminate_signal = 1;
    }

    lprintf("\n");
    stopwatch_stop(&sw);
    gap = sw.elapsed;
    LOG_PRINT_TIME(gap, " sec elapsed\n");
    gap_double = gap.tv_sec + (double)gap.tv_usec / 1000000.0;

    for (i=0;i<bench_threads;++i){
        thread_join(bench_worker[i], &bench_worker_ret[i]);
    }

    // waiting for unterminated compactor & bench workers
    if (cur_compaction != -1) {
        lprintf("waiting for termination of remaining compaction..\n");
        fflush(stdout);
        thread_join(tid_compactor, &compactor_ret);
    }

    lprintf("%d reads (%.2f ops/sec)\n"
            "%d writes (%.2f ops/sec)\n",
            op_count_read, (double)op_count_read / gap_double,
            op_count_write, (double)op_count_write / gap_double);

    lprintf("total %d operations (%.2f ops/sec) performed\n",
            op_count_read + op_count_write,
             (double)(op_count_read + op_count_write) / gap_double);

#if defined(__FDB_BENCH) || defined(__COUCH_BENCH)
    if (!binfo->auto_compaction) {
        // manual compaction
        lprintf("compaction : occurred %d time%s, ",
                total_compaction, (total_compaction>1)?("s"):(""));
        LOG_PRINT_TIME(sw_compaction.elapsed, " sec elapsed\n");
    }
#endif

    written_final = print_proc_io_stat(cmd);
#if defined(__PRINT_IOSTAT)
    {
        uint64_t written = written_final - written_init;
        uint64_t w_per_doc = (double)written / op_count_write;
        uint64_t avg_docsize;

        if (op_count_write) {
            lprintf("total %"_F64" bytes (%s) written during benchmark\n", written,
                    print_filesize_approx((written_final - written_init), bodybuf));
            if (binfo->bodylen.type == RND_NORMAL) {
                avg_docsize = binfo->bodylen.a;
            } else {
                avg_docsize = (binfo->bodylen.a + binfo->bodylen.b)/2;
            }
            lprintf("average disk write throughput: %.2f MB/s\n",
                    (double)written / (gap.tv_sec*1000000 + gap.tv_usec) *
                        1000000 / (1024*1024));
            lprintf("%s written per doc update (%.1fx write amplification)\n",
                    print_filesize_approx(w_per_doc, bodybuf),
                    (double)w_per_doc / avg_docsize);
        }
    }
#endif

    lprintf("\n");

    keygen_free(&binfo->keygen);
    if (binfo->batch_dist.type == RND_ZIPFIAN) {
        zipf_rnd_free(&zipf);
    }

#ifdef __FDB_BENCH
    // print ForestDB's own block cache info (internal function call)
    //bcache_print_items();
#endif

    printf("waiting for termination of DB module..\n");
#if defined(__FDB_BENCH) || defined(__COUCH_BENCH) || defined(__WT_BENCH)
    for (i=0;i<bench_threads;++i){
        for (j=0;j<binfo->nfiles;++j){
            couchstore_close_db(b_args[i].db[j]);
#ifdef __FDB_BENCH
            if (i==0) {
                couchstore_close_db(info_handle[j]);
            }
#endif
        }
        free(b_args[i].db);
    }
#elif defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
    for (j=0;j<binfo->nfiles;++j){
        couchstore_close_db(b_args[0].db[j]);
    }
    free(b_args[0].db);
#endif

#if defined(__WT_BENCH) || defined(__FDB_BENCH)
    couchstore_close_conn();
#endif

    free(dbinfo);

    _bench_result_print(&result);
    _bench_result_free(&result);

    memleak_end();
}

void _print_benchinfo(struct bench_info *binfo)
{
    char tempstr[256];

    lprintf("\n === benchmark configuration ===\n");
    lprintf("DB module: ");
#ifdef __FDB_BENCH
    lprintf("ForestDB\n");
#elif __COUCH_BENCH
    lprintf("Couchstore\n");
#elif __LEVEL_BENCH
    lprintf("LevelDB\n");
#elif __ROCKS_BENCH
    lprintf("RocksDB\n");
#elif __WT_BENCH
    lprintf("WiredTiger\n");
#else
    lprintf("unknown\n");
#endif

    lprintf("random seed: %d\n", (int)rnd_seed);

    if (strcmp(binfo->init_filename, binfo->filename)) {
        lprintf("initial filename: %s#\n", binfo->init_filename);
    }
    lprintf("filename: %s#", binfo->filename);
    if (binfo->initialize) {
        lprintf(" (initialize)\n");
    } else {
        lprintf(" (use existing DB file)\n");
    }

    lprintf("# documents (i.e. working set size): %d\n", (int)binfo->ndocs);
    if (binfo->nfiles > 1) {
        lprintf("# files: %d\n", (int)binfo->nfiles);
    }

    lprintf("# threads: ");
    lprintf("reader %d", (int)binfo->nreaders);
    if (binfo->write_prob > 100) {
        if (binfo->reader_ops) {
            lprintf(" (%d ops/sec), ", (int)binfo->reader_ops);
        } else {
            lprintf(" (max), ");
        }
    } else {
        lprintf(", ");
    }
    lprintf("writer %d", (int)binfo->nwriters);
    if (binfo->write_prob > 100) {
        if (binfo->writer_ops) {
            lprintf(" (%d ops/sec)\n", (int)binfo->writer_ops);
        } else {
            lprintf(" (max)\n");
        }
    } else {
        lprintf("\n");
    }

    lprintf("block cache size: %s\n",
            print_filesize_approx(binfo->cache_size, tempstr));
#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH)
    lprintf("WBS size: %s (init), ",
        print_filesize_approx(binfo->wbs_init, tempstr));
    lprintf("%s (bench)\n",
        print_filesize_approx(binfo->wbs_bench, tempstr));
#endif
#if defined(__FDB_BENCH)
    lprintf("WAL size: %"_F64"\n", binfo->fdb_wal);
#endif
#if defined(__WT_BENCH)
    lprintf("indexing: %s\n", (binfo->wt_type==0)?"b-tree":"lsm-tree");
#endif

    lprintf("key length: %s(%d,%d) / ",
            (binfo->keylen.type == RND_NORMAL)?"Norm":"Uniform",
            (int)binfo->keylen.a, (int)binfo->keylen.b);
    lprintf("body length: %s(%d,%d)\n",
            (binfo->bodylen.type == RND_NORMAL)?"Norm":"Uniform",
            (int)binfo->bodylen.a, (int)binfo->bodylen.b);

    lprintf("batch distribution: ");
    if (binfo->batch_dist.type == RND_UNIFORM) {
        lprintf("Uniform\n");
    }else{
        lprintf("Zipfian (s=%.2f, group: %d documents)\n",
                (double)binfo->batch_dist.a/100.0, (int)binfo->batch_dist.b);
    }

    if (binfo->nbatches > 0) {
        lprintf("# batches for benchmark: %lu\n",
            (unsigned long)binfo->nbatches);
    }
    if (binfo->nops > 0){
        lprintf("# operations for benchmark: %lu\n",
                (unsigned long)binfo->nops);
    }
    if (binfo->bench_secs > 0){
        lprintf("benchmark duration: %lu seconds\n", (unsigned long)binfo->bench_secs);
    }

    lprintf("read batch size: %s(%d,%d) / ",
            (binfo->rbatchsize.type == RND_NORMAL)?"Norm":"Uniform",
            (int)binfo->rbatchsize.a, (int)binfo->rbatchsize.b);
    lprintf("write batch size: %s(%d,%d)\n",
            (binfo->wbatchsize.type == RND_NORMAL)?"Norm":"Uniform",
            (int)binfo->wbatchsize.a, (int)binfo->wbatchsize.b);
    lprintf("inside batch distribution: %s",
            (binfo->op_dist.type == RND_NORMAL)?"Norm":"Uniform");
    lprintf(" (-%d ~ +%d, total %d)\n",
            (int)binfo->batchrange , (int)binfo->batchrange, (int)binfo->batchrange*2);
    if (binfo->write_prob <= 100) {
        lprintf("write ratio: %d %%", (int)binfo->write_prob);
    } else {
        lprintf("write ratio: max capacity");
    }
    lprintf(" (%s)\n", ((binfo->sync_write)?("synchronous"):("asynchronous")));

#if defined(__FDB_BENCH)
    lprintf("compaction threshold: %d %% "
            "(period: %d seconds, %s)\n",
            (int)binfo->compact_thres, (int)binfo->compact_period,
            ((binfo->auto_compaction)?("auto"):("manual")));
#endif
#if defined(__COUCH_BENCH)
    lprintf("compaction threshold: %d %%\n", (int)binfo->compact_thres);
#endif
#if defined(__WT_BENCH)
    lprintf("checkpoint period: %d seconds\n", (int)binfo->compact_period);
#endif
}

void _set_keygen(struct bench_info *binfo)
{
    int i, level = binfo->nlevel+1;
    int avg_keylen, avg_prefixlen;
    struct rndinfo rnd_len[level], rnd_dist[level];
    struct keygen_option opt;

    if (binfo->keylen.type == RND_NORMAL) {
        avg_keylen = binfo->keylen.a;
    } else {
        avg_keylen = (binfo->keylen.a + binfo->keylen.b) / 2;
    }
    if (binfo->prefixlen.type == RND_NORMAL) {
        avg_prefixlen = binfo->prefixlen.a;
    } else {
        avg_prefixlen = (binfo->prefixlen.a + binfo->prefixlen.b) / 2;
    }

    for (i=0;i<binfo->nlevel+1; ++i) {
        if (i<binfo->nlevel) {
            rnd_len[i] = binfo->prefixlen;
            rnd_dist[i].type = RND_UNIFORM;
            rnd_dist[i].a = 0;
            rnd_dist[i].b = binfo->nprefixes;
        } else {
            // right most (last) prefix
            rnd_len[i].type = RND_NORMAL;
            rnd_len[i].a = avg_keylen - avg_prefixlen * binfo->nlevel;
            rnd_len[i].b = binfo->keylen.b;
            rnd_dist[i].type = RND_UNIFORM;
            rnd_dist[i].a = 0;
            rnd_dist[i].b = 0xfffffffffffffff;
        }
    }
    opt.abt_only = 1;
    opt.delimiter = 1;
    keygen_init(&binfo->keygen, level, rnd_len, rnd_dist, &opt);
}

struct bench_info get_benchinfo()
{
    static dictionary *cfg;
    cfg = iniparser_new((char*)"./bench_config.ini");

    struct bench_info binfo;
    char *str;
    char *filename = (char*)malloc(256);
    char *init_filename = (char*)malloc(256);
    char *log_filename = (char*)malloc(256);
    size_t ncores;
#if defined(WIN32) || defined(_WIN32)
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    ncores = (size_t)sysinfo.dwNumberOfProcessors;
#else
    ncores = (size_t)sysconf(_SC_NPROCESSORS_ONLN);
#endif

    binfo.ndocs = iniparser_getint(cfg, (char*)"document:ndocs", 10000);
    binfo.filename = filename;
    binfo.init_filename = init_filename;
    binfo.log_filename = log_filename;

    str = iniparser_getstring(cfg, (char*)"log:filename", (char*)"");
    strcpy(binfo.log_filename, str);

    binfo.cache_size = iniparser_getint(cfg, (char*)"db_config:cache_size_MB", 128);
    binfo.cache_size *= (1024*1024);

    str = iniparser_getstring(cfg, (char*)"db_config:compaction_mode", (char*)"auto");
    if (str[0] == 'a' || str[0] == 'A') binfo.auto_compaction = 1;
    else binfo.auto_compaction = 0;
#if defined(__LEVEL_BENCH) || defined(__ROCKS_BENCH) || defined(__WT_BENCH)
    binfo.auto_compaction = 1;
#elif defined(__COUCH_BENCH)
    // couchstore: manual compaction only
    binfo.auto_compaction = 0;
#endif

    binfo.wbs_init = iniparser_getint(cfg, (char*)"db_config:wbs_init_MB", 4);
    binfo.wbs_init *= (1024*1024);
    binfo.wbs_bench = iniparser_getint(cfg, (char*)"db_config:wbs_bench_MB", 4);
    binfo.wbs_bench *= (1024*1024);
    binfo.fdb_wal = iniparser_getint(cfg, (char*)"db_config:fdb_wal", 4096);
    str = iniparser_getstring(cfg, (char*)"db_config:wt_type", (char*)"btree");
    if (str[0] == 'b' || str[0] == 'B') {
        binfo.wt_type = 0; /* b-tree */
    } else {
        binfo.wt_type = 1; /* lsm-tree */
    }

    str = iniparser_getstring(cfg, (char*)"db_file:filename", (char*)"./dummy");
    strcpy(binfo.filename, str);

    str = iniparser_getstring(cfg, (char*)"db_file:init_filename", binfo.filename);
    strcpy(binfo.init_filename, str);

    binfo.nfiles = iniparser_getint(cfg, (char*)"db_file:nfiles", 1);

    binfo.pop_nthreads = iniparser_getint(cfg, (char*)"population:nthreads", ncores*2);
    if (binfo.pop_nthreads < 1) binfo.pop_nthreads = ncores*2;
    if (binfo.pop_nthreads > binfo.nfiles) binfo.pop_nthreads = binfo.nfiles;

    binfo.pop_batchsize = iniparser_getint(cfg, (char*)"population:batchsize", 4096);

    str = iniparser_getstring(cfg, (char*)"population:periodic_commit", (char*)"no");
    if (str[0] == 'n' /*|| binfo.nthreads == 1*/) binfo.pop_commit = 0;
    else binfo.pop_commit = 1;

    str = iniparser_getstring(cfg, (char*)"population:fdb_flush_wal", (char*)"no");
    if (str[0] == 'n' /*|| binfo.nthreads == 1*/) binfo.fdb_flush_wal = 0;
    else binfo.fdb_flush_wal = 1;

    // key length
    str = iniparser_getstring(cfg, (char*)"key_length:distribution", (char*)"normal");
    if (str[0] == 'n') {
        binfo.keylen.type = RND_NORMAL;
        binfo.keylen.a = iniparser_getint(cfg, (char*)"key_length:median", 64);
        binfo.keylen.b = iniparser_getint(cfg,
                                          (char*)"key_length:standard_deviation", 8);
    }else{
        binfo.keylen.type = RND_UNIFORM;
        binfo.keylen.a = iniparser_getint(cfg, (char*)"key_length:lower_bound", 32);
        binfo.keylen.b = iniparser_getint(cfg, (char*)"key_length:upper_bound", 96);
    }

    // prefix composition
    str = iniparser_getstring(cfg, (char*)"prefix:distribution", (char*)"uniform");
    if (str[0] == 'n') {
        binfo.prefixlen.type = RND_NORMAL;
        binfo.prefixlen.a = iniparser_getint(cfg, (char*)"prefix:median", 8);
        binfo.prefixlen.b = iniparser_getint(cfg,
                                             (char*)"prefix:standard_deviation", 1);
    }else{
        binfo.prefixlen.type = RND_UNIFORM;
        binfo.prefixlen.a = iniparser_getint(cfg, (char*)"prefix:lower_bound", 4);
        binfo.prefixlen.b = iniparser_getint(cfg, (char*)"prefix:upper_bound", 12);
    }
    binfo.nlevel = iniparser_getint(cfg, (char*)"prefix:level", 0);
    binfo.nprefixes = iniparser_getint(cfg, (char*)"prefix:nprefixes", 100);

    // thread information
    binfo.nreaders = iniparser_getint(cfg, (char*)"threads:readers", 0);
    binfo.nwriters = iniparser_getint(cfg, (char*)"threads:writers", 0);
    binfo.reader_ops = iniparser_getint(cfg, (char*)"threads:reader_ops", 0);
    binfo.writer_ops = iniparser_getint(cfg, (char*)"threads:writer_ops", 0);

    // create keygen structure
    _set_keygen(&binfo);

    str = iniparser_getstring(cfg, (char*)"body_length:distribution", (char*)"normal");
    if (str[0] == 'n') {
        binfo.bodylen.type = RND_NORMAL;
        binfo.bodylen.a = iniparser_getint(cfg, (char*)"body_length:median", 512);
        binfo.bodylen.b = iniparser_getint(cfg,
                                           (char*)"body_length:standard_deviation",
                                           32);
    }else{
        binfo.bodylen.type = RND_UNIFORM;
        binfo.bodylen.a = iniparser_getint(cfg, (char*)"body_length:lower_bound", 448);
        binfo.bodylen.b = iniparser_getint(cfg, (char*)"body_length:upper_bound", 576);
    }

    binfo.nbatches = iniparser_getint(cfg, (char*)"operation:nbatches", 0);
    binfo.nops = iniparser_getint(cfg, (char*)"operation:nops", 0);
    binfo.bench_secs = iniparser_getint(cfg, (char*)"operation:duration", 0);
    if (binfo.nbatches == 0 && binfo.nops == 0 && binfo.bench_secs == 0) {
        binfo.bench_secs = 60;
    }

    size_t avg_write_batchsize;
    str = iniparser_getstring(cfg, (char*)"operation:batchsize_distribution",
                              (char*)"normal");
    if (str[0] == 'n') {
        binfo.rbatchsize.type = RND_NORMAL;
        binfo.rbatchsize.a = iniparser_getint(cfg,
                                              (char*)"operation:read_batchsize_median",
                                              3);
        binfo.rbatchsize.b =
            iniparser_getint(cfg,
                             (char*)"operation:read_batchsize_standard_deviation",
                             1);
        binfo.wbatchsize.type = RND_NORMAL;
        binfo.wbatchsize.a =
            iniparser_getint(cfg,
                             (char*)"operation:write_batchsize_median", 10);
        binfo.wbatchsize.b =
            iniparser_getint(cfg,
                             (char*)"operation:write_batchsize_standard_deviation",
                             1);
        avg_write_batchsize = binfo.wbatchsize.a;
    }else{
        binfo.rbatchsize.type = RND_UNIFORM;
        binfo.rbatchsize.a =
            iniparser_getint(cfg,
                             (char*)"operation:read_batchsize_lower_bound", 1);
        binfo.rbatchsize.b =
            iniparser_getint(cfg,
                             (char*)"operation:read_batchsize_upper_bound", 5);
        binfo.wbatchsize.type = RND_UNIFORM;
        binfo.wbatchsize.a =
            iniparser_getint(cfg,
                             (char*)"operation:write_batchsize_lower_bound", 5);
        binfo.wbatchsize.b =
            iniparser_getint(cfg,
                             (char*)"operation:write_batchsize_upper_bound", 15);
        avg_write_batchsize = (binfo.wbatchsize.a + binfo.wbatchsize.b)/2;
    }

    str = iniparser_getstring(cfg, (char*)"operation:read_query", (char*)"key");
    if (str[0] == 'k' || str[0] == 'i') {
        binfo.read_query_byseq = 0;
    }else {
        // by_seq is not supported now..
        //binfo.read_query_byseq = 1;
        binfo.read_query_byseq = 0;
    }

    str = iniparser_getstring(cfg,
                              (char*)"operation:batch_distribution",
                              (char*)"uniform");
    if (str[0] == 'u') {
        binfo.batch_dist.type = RND_UNIFORM;
        binfo.batch_dist.a = 0;
        binfo.batch_dist.b = binfo.ndocs;
    }else{
        double s = iniparser_getdouble(cfg, (char*)"operation:batch_parameter1", 1);
        binfo.batch_dist.type = RND_ZIPFIAN;
        binfo.batch_dist.a = (int64_t)(s * 100);
        binfo.batch_dist.b = iniparser_getint(cfg,
                                              (char*)"operation:batch_parameter2",
                                              64);
    }

    str = iniparser_getstring(cfg,
                              (char*)"operation:operation_distribution",
                              (char*)"uniform");
    if (str[0] == 'n') {
        binfo.op_dist.type = RND_NORMAL;
    }else{
        binfo.op_dist.type = RND_UNIFORM;
    }

    binfo.batchrange = iniparser_getint(cfg, (char*)"operation:batch_range",
                                        avg_write_batchsize);

    binfo.write_prob = iniparser_getint(cfg,
                                        (char*)"operation:write_ratio_percent",
                                        20);
    if (binfo.write_prob == 0) {
        binfo.nwriters = 0;
    } else if (binfo.write_prob == 100) {
        binfo.nreaders = 0;
    }

    str = iniparser_getstring(cfg, (char*)"operation:write_type", (char*)"sync");
    binfo.sync_write = (str[0]=='s')?(1):(0);

    binfo.compact_thres = iniparser_getint(cfg, (char*)"compaction:threshold", 30);
    binfo.compact_period = iniparser_getint(cfg, (char*)"compaction:period", 15);

    iniparser_free(cfg);

    return binfo;
}

int main(int argc, char **argv){
    char filename[256];
    struct bench_info binfo;
    struct timeval gap;

    randomize();
    rnd_seed = rand();

    binfo = get_benchinfo();

    if (strcmp(binfo.log_filename, "")){
        int ret;
        char temp[256], cmd[256], *str;

        // create directory if doesn't exist
        str = _get_dirname(binfo.log_filename, temp);
        if (str) {
            if (!_does_file_exist(str)) {
                sprintf(cmd, "mkdir -p %s > errorlog.txt", str);
                ret = system(cmd);
            }
        }

        // open ops log file
        gettimeofday(&gap, NULL);
        sprintf(filename, "%s_%d.txt", binfo.log_filename, (int)gap.tv_sec);
        log_fp = fopen(filename, "w");
    }

    binfo.initialize = 1;
    if (argc > 1) {
        if (!memcmp((void *)argv[1], (void*)"--e", 3) ||
            !memcmp((void *)argv[1], (void*)"-e", 2)) {
            // use existing DB file
            binfo.initialize = 0;
        }
    }

    _print_benchinfo(&binfo);
    do_bench(&binfo);

    if (log_fp) {
        fclose(log_fp);
    }

    return 0;
}

