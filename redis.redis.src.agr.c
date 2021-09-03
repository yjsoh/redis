// agr.c
/**
 * @brief Cyborg version of AGR. Attempting to strictly follow the potential implementation.
 *
 * agr_*() functions returns the roll forward, roll backward, discard, or ignore options.
 */
#include "sds.h"
#include "sdsalloc.h"

#include "server.h"
#include "pmem.h"
#include <libpmemobj.h>

/* dbOverwritePM */
void agr_pmemKVpairSet(void *key, void *val)
{
    PMEMoid *kv_PM_oid;
    PMEMoid val_oid;
    struct key_val_pair_PM *kv_PM_p;

    kv_PM_oid = sdsPMEMoidBackReference((sds)key);
    kv_PM_p = (struct key_val_pair_PM *)pmemobj_direct(*kv_PM_oid);

    val_oid.pool_uuid_lo = server.pool_uuid_lo;
    val_oid.off = (uint64_t)val - (uint64_t)server.pm_pool->addr;

    TX_ADD_FIELD_DIRECT(kv_PM_p, val_oid);
    kv_PM_p->val_oid = val_oid;
    return;
}

int agr_dictReplacePM(dict *d, void *key, void *val)
{
    dictEntry *entry, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will suceed. */
    if (dictAddPM(d, key, val) == DICT_OK)
        return 1;
    /* It already exists, get the entry */
    entry = dictFind(d, key);
    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
    auxentry = *entry;
    dictSetVal(d, entry, val);
    agr_pmemKVpairSet(entry->key, ((robj *)val)->ptr);
    dictFreeVal(d, &auxentry);
    return 0;
}

void agr_dbOverwritePM(redisDb *db, robj *key, robj *val) {
    dictEntry *de = dictFind(db->dict,key->ptr);

    serverAssertWithInfo(NULL,key,de != NULL);
    agr_dictReplacePM(db->dict, key->ptr, val);
}

/* dbAddPM */
void agr_sdsdupPM(const sds s, void **oid_reference) {
    sds new_sds;
    new_sds = agr_sdsnewlenPM(s, sdslen(s));
    *oid_reference = (void *)sdsPMEMoidBackReference(new_sds);
    return new_sds;
}

void agr_dictAddPM() {
    dictEntry *entry = dictAddRawPM(d,key);

    if (!entry) return DICT_ERR;
    dictSetVal(d, entry, val);

    return DICT_OK;
}

void agr_pmemAddToPmemList(void *key, void *val) {
    PMEMoid key_oid;
    PMEMoid val_oid;
    PMEMoid kv_PM;
    struct key_val_pair_PM *kv_PM_p;
    TOID(struct key_val_pair_PM) typed_kv_PM;
    struct redis_pmem_root *root;

    key_oid.pool_uuid_lo = server.pool_uuid_lo;
    // key_oid.off = (uint64_t)key - (uint64_t)server.pm_pool->addr; // key is volatile, thus ignored

    val_oid.pool_uuid_lo = server.pool_uuid_lo;
    // val_oid.off = (uint64_t)val - (uint64_t)server.pm_pool->addr; // val is volatile, thus ignored

    kv_PM = pmemobj_tx_zalloc(sizeof(struct key_val_pair_PM), pm_type_key_val_pair_PM);
    kv_PM_p = (struct key_val_pair_PM *)pmemobj_direct(kv_PM);
    // kv_PM_p->key_oid = key_oid; // key_oid is incomplete thus ignored
    // kv_PM_p->val_oid = val_oid; // val_oid is incomplete thus ignored
    typed_kv_PM.oid = kv_PM;

    root = pmemobj_direct(server.pm_rootoid.oid); // available. server.pm_rootoid is a PM address

    kv_PM_p->pmem_list_next = root->pe_first;
    if(!TOID_IS_NULL(root->pe_first)) {
        struct key_val_pair_PM *head = D_RW(root->pe_first);
        TX_ADD_FIELD_DIRECT(head,pmem_list_prev);
    	head->pmem_list_prev = typed_kv_PM;
    }

    // assuming that this is undo-logging
    TX_ADD_DIRECT(root);
    root->pe_first = typed_kv_PM;
    root->num_dict_entries++;

    return kv_PM;
}

void agr_dbAddPM(redisDb *db, robj *key, robj *val) {
    // PMEMoid kv_PM; // not used thus ignored
    PMEMoid *kv_pm_reference;

    // sds copy = sdsdupPM(key->ptr, (void **) &kv_pm_reference); // return value used in subsequent agr_functions
    sds copy;
    agr_sdsdupPM(key->ptr, (void **) &kv_pm_reference, &copy);

    // int retval = dictAddPM(db->dict, copy, val); volatile thus return value ignored
    agr_dictAddPM(db->dict, copy, val);

    // kv_PM = pmemAddToPmemList((void *)copy, (void *)(val->ptr)); // volatile thus return value ignored
    agr_pmemAddToPmemList((void*) copy, (void *) val->ptr);

    // *kv_pm_reference = kv_PM; // volatile thus ignored

    // serverAssertWithInfo(NULL,key,retval == C_OK); // volatile thus ignored
    // if (val->type == OBJ_LIST) signalListAsReady(db, key); // volatile thus ignored
    // if (server.cluster_enabled) slotToKeyAdd(key); // volatile thus ignored
}

void agr_setKeyPM(redisDb *db, robj *key, robj *val) {
    if(lookupKeyWrite(db,key) == NULL) {
        agr_dbAddPM(db, key, val);
    } else {
        agr_dbOverwritePM(db, key, val);
    }

    removeExpire(db,key); // TBD: volatile thus comment out?
    signalModifiedKey(db,key); // TBD: volatile thus comment out?
}

void agr_sdsnewlenPM(const void *init, size_t initlen) {
    void *sh;
    sds s;
    char type = sdsReqType(initlen);
    PMEMoid oid;
    /* Empty strings are usually created in order to append. Use type 8
     * since type 5 is not good at this. */
    if (type == SDS_TYPE_5 && initlen == 0) type = SDS_TYPE_8;
    int hdrlen = sdsHdrSize(type);
    unsigned char *fp; /* flags pointer. */

    hdrlen += sizeof(PMEMoid);
    oid = pmemobj_tx_zalloc((hdrlen+initlen+1),PM_TYPE_SDS);
    sh = pmemobj_direct(oid);

    if (!init)
        memset(sh, 0, hdrlen+initlen+1);
    if (sh == NULL) return NULL;
    s = (char*)sh+hdrlen;
    fp = ((unsigned char*)s)-1;
    switch(type) {
        case SDS_TYPE_5: {
            *fp = type | (initlen << SDS_TYPE_BITS);
            break;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8,s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16,s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32,s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64,s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
    }
    if (initlen && init)
        memcpy(s, init, initlen);
    s[initlen] = '\0';
    return s;
}

/* Create a string object with encoding OBJ_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string.
 * Located in PM */
void agr_createRawStringObjectPM(const char *ptr, size_t len) {
    int agr_tmp = agr_sdsnewlenPM(ptr, len);
    // agr_createObjectPM(OBJ_STRING, agr_tmp); // voltaile thus comment out
    return;
}

void agr_dupStringObjectPM(robj *o) {
    robj *d;

    // serverAssert(o->type == OBJ_STRING);

    switch(o->encoding) {
    case OBJ_ENCODING_RAW:
    case OBJ_ENCODING_EMBSTR:
        // return createRawStringObjectPM(o->ptr,sdslen(o->ptr));
        agr_createRawStringObjectPM(o->ptr, sdslen(o->ptr));
        /* return createEmbeddedStringObjectPM(o->ptr,sdslen(o->ptr)); */
        return;
    case OBJ_ENCODING_INT:
        // d = createObjectPM(OBJ_STRING, NULL); // volatile thus ignored
        // d->encoding = OBJ_ENCODING_INT;
        // d->ptr = o->ptr;
        // return d;
        return;
    default:
        serverPanic("Wrong encoding.");
        break;
    }
}

/*
TX_BEGIN(server.pm_pool) {
    newVal = dupStringObjectPM(val);
    // Set key in PM - create DictEntry and sds(key) linked to RedisObject with value
    // Don't increment value "ref counter" as in normal process.s
    setKeyPM(c->db,key,newVal);
} TX_ONABORT {
    error = 1;
} TX_END
*/
void main() {
    // read from the PM

    // recover for the PM
    agr_dupStringObjectPM(val);
    agr_setKeyPM(c->db,key,newVal);
}