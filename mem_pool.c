/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _alloc {
    char *mem;
    size_t size;
} alloc_t, *alloc_pt;

typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;


/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size, node_pt node);
static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                           size_t size, node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status _mem_invalidate_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    if(pool_store_capacity > 0)
    {
        return ALLOC_CALLED_AGAIN;
    }

    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate
    pool_store = (pool_mgr_pt*) calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_t));
    if(pool_store == NULL)
    {
        return ALLOC_FAIL;
    }
    pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;

    return ALLOC_OK;
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if(pool_store_capacity == 0)
    {
        return ALLOC_CALLED_AGAIN;
    }

    // make sure all pool managers have been deallocated

    // can free the pool store array
    free(*pool_store);

    // update static variables
    *pool_store = NULL; // an array of pointers, only expand
    pool_store_size = 0;
    pool_store_capacity = 0;

    return ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    assert(pool_store_capacity > 0);

    // expand the pool store, if necessary
    if(pool_store_size == pool_store_capacity)
    {
        _mem_resize_pool_store();
    }

    // allocate a new mem pool mgr
    pool_mgr_pt new_pool_mgr = (pool_mgr_pt) malloc(sizeof(pool_mgr_t));
    // check success, on error return null
    if(new_pool_mgr == NULL)
    {
      return NULL;
    }

    // allocate a new memory pool
    new_pool_mgr->pool.mem = malloc(size);
    // check success, on error deallocate mgr and return null
    if(new_pool_mgr->pool.mem == NULL)
    {
        free(new_pool_mgr);
        return NULL;
    }

    // allocate a new node heap
    new_pool_mgr->node_heap = (node_pt) calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    // check success, on error deallocate mgr/pool and return null
    if(new_pool_mgr->node_heap == NULL)
    {
        free(new_pool_mgr->pool.mem);
        free(new_pool_mgr);
        return NULL;
    }

    // allocate a new gap index
    new_pool_mgr->gap_ix = (gap_pt) calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    // check success, on error deallocate mgr/pool/heap and return null
    if(new_pool_mgr->gap_ix == NULL)
    {
        free(new_pool_mgr->node_heap);
        free(new_pool_mgr->pool.mem);
        free(new_pool_mgr);
      return NULL;
    }

    // assign all the pointers and update meta data:

    //   initialize top node of node heap
    new_pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    new_pool_mgr->used_nodes = 1;
    new_pool_mgr->node_heap[0].alloc_record.mem = new_pool_mgr->pool.mem;
    new_pool_mgr->node_heap[0].alloc_record.size = size;
    new_pool_mgr->node_heap[0].used = 1;
    new_pool_mgr->node_heap[0].allocated = 0;
    new_pool_mgr->node_heap[0].next = NULL;
    new_pool_mgr->node_heap[0].prev = NULL;
    //   initialize top node of gap index
    new_pool_mgr->gap_ix_capacity =  MEM_GAP_IX_INIT_CAPACITY;
    new_pool_mgr->gap_ix[0].size = 1;
    _mem_add_to_gap_ix(new_pool_mgr, size, &new_pool_mgr->node_heap[0]);

    //   initialize pool mgr
    new_pool_mgr->pool.policy = policy;
    new_pool_mgr->pool.total_size = size;
    new_pool_mgr->pool.alloc_size = 0;
    new_pool_mgr->pool.num_allocs = 0;
    new_pool_mgr->pool.num_gaps = 1;

    //   link pool mgr to pool store
    pool_store[pool_store_size++] = new_pool_mgr;

    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) new_pool_mgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)

    // check if this pool is allocated
    // check if pool has only one gap
    // check if it has zero allocations
    if(pool->mem != NULL && pool->num_gaps != 1 && pool->num_allocs != 0)
    {
        return ALLOC_NOT_FREED;
    }
        // free memory pool
        free(pool->mem);

        // free node heap
        free(((pool_mgr_pt)pool)->node_heap);

        // free gap index
        free(((pool_mgr_pt)pool)->gap_ix);


    // find mgr in pool store and set to null
    for(int i = 0; i < pool_store_capacity; i++) {

        if (pool_store[i] == ((pool_mgr_pt) pool)) {

            pool_store[i] = NULL;
        }
    }
    // note: don't decrement pool_store_size, because it only grows

    // free mgr
    free((pool_mgr_pt)pool);

    return ALLOC_OK;
}

void * mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;

    int x = 0;

    size_t rem_gap_size = 0;

    // check if any gaps, return null if none
    if(pool->num_gaps == 0)
    {
        return NULL;
    }
    // expand heap node, if necessary, quit on error
    _mem_resize_node_heap(pool_mgr);
    // check used nodes fewer than total nodes, quit on error
    if(pool_mgr->used_nodes > pool_mgr->total_nodes)
    {
        return NULL;
    }

    // get a node for allocation:
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if(pool->policy == 0)
    {
        for(int i = 0; i < pool_mgr->used_nodes; i++)
        {
            // Check if node found
            if(pool_mgr->node_heap[i].allocated == 0)
            {
                if(pool_mgr->node_heap[i].alloc_record.size >= size)
                {
                    x = i;
                    break;
                }
            }
        }

    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    if(pool->policy == 1)
    {
        // check if node found
        for (int i = 0; i < pool_mgr->total_nodes; i++)
        {
            if (pool_mgr->gap_ix[i].node->allocated == 0)
            {
                if (pool_mgr->node_heap[i].alloc_record.size >= size)
                {
                    x = i;
                    break;
                }
            }
        }
    }
    // update metadata (num_allocs, alloc_size)
    pool->num_allocs += 1;
    pool->alloc_size += size;

    // calculate the size of the remaining gap, if any
    if(pool_mgr->node_heap[x].alloc_record.size > size)
    {
        rem_gap_size = pool_mgr->node_heap[x].alloc_record.size - size;
    }

    // remove node from gap index
    _mem_remove_from_gap_ix(pool_mgr, size, &pool_mgr->node_heap[x]);

    // convert gap_node to an allocation node of given size
    pool_mgr->node_heap[x].allocated = 1;
    pool_mgr->node_heap[x].alloc_record.size = size;

    // adjust node heap:
    //   if remaining gap, need a new node
    //   find an unused one in the node heap
    if(rem_gap_size != 0 )
    {
        for (int i = 0; i < pool_mgr->used_nodes; i++)
        {
            //   make sure one was found
            if (pool_mgr->node_heap[i].used == 0)
            {
                //   initialize it to a gap node
                pool_mgr->node_heap[i].allocated = 0;
                pool_mgr->node_heap[i].used = 1;
                pool_mgr->node_heap[i].alloc_record.size = rem_gap_size;
                pool_mgr->node_heap[i].alloc_record.mem += size;

                //   update linked list (new node right after the node for allocation)
               pool_mgr->node_heap[x].next = &(pool_mgr->node_heap[i]);
               pool_mgr->node_heap[i].prev = &(pool_mgr->node_heap[x]);

                //   add to gap index
               _mem_add_to_gap_ix(pool_mgr, rem_gap_size, &pool_mgr->node_heap[i]);

                //   check if successful

            }
        }
    }

    //   update metadata (used_nodes)
    pool_mgr->used_nodes += 1;

    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt)pool_mgr->node_heap;
}

alloc_status mem_del_alloc(pool_pt pool, void * alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt)alloc;

    // find the node in the node heap
    // this is node-to-delete
    for(int i = 0; i < pool_mgr->used_nodes; i++)
    {
        // make sure it's found
        if(&pool_mgr->node_heap[i] == node)
        {
            // convert to gap node
            pool_mgr->node_heap[i].allocated = 0;

            // update metadata (num_allocs, alloc_size)
            pool->num_allocs -= 1;
            pool->alloc_size -= pool_mgr->node_heap[i].alloc_record.size;

            // if the next node in the list is also a gap, merge into node-to-delete
            if(pool_mgr->node_heap[i].next->allocated == 0)
            {
                //   remove the next node from gap index
                //   check success
                _mem_remove_from_gap_ix(pool_mgr, pool_mgr->node_heap[i].next->alloc_record.size, pool_mgr->node_heap[i].next);
                //   add the size to the node-to-delete
                pool_mgr->node_heap[i].alloc_record.size += pool_mgr->node_heap[i].next->alloc_record.size;
                //   update node as unused
                pool_mgr->node_heap[i].next->used = 0;
                //   update metadata (used nodes)
                pool_mgr->used_nodes -= 1;

                //   update linked list:

                //if (next->next) {
                if(pool_mgr->node_heap[i].next->next)
                {
                    //next->next->prev = node_to_del;
                    pool_mgr->node_heap[i].next->next->prev = &pool_mgr->node_heap[i];
                    //node_to_del->next = next->next;
                    pool_mgr->node_heap[i].next = pool_mgr->node_heap[i].next->next;

                }
                else
                {
                    //node_to_del->next = NULL;
                    pool_mgr->node_heap[i].next = NULL;
                }
                //next->next = NULL;
                pool_mgr->node_heap[i].next->next = NULL;
                //next->prev = NULL;
                pool_mgr->node_heap[i].next->prev = NULL;

                // this merged node-to-delete might need to be added to the gap index
                // but one more thing to check...

            }
            // if the previous node in the list is also a gap, merge into previous!
            if(pool_mgr->node_heap[i].prev->allocated == 0)
            {
                //   remove the previous node from gap index
                //   check success
                _mem_remove_from_gap_ix(pool_mgr, pool_mgr->node_heap[i--].alloc_record.size, &pool_mgr->node_heap[i--]);
                //   add the size of node-to-delete to the previous
                pool_mgr->node_heap[i--].alloc_record.size += pool_mgr->node_heap[i].alloc_record.size;
                //   update node-to-delete as unused
                pool_mgr->node_heap[i].used = 0;
                //   update metadata (used nodes)
                pool_mgr->used_nodes -= 1;

                //if (node_to_del->next) {
                if(pool_mgr->node_heap[i].next)
                {
                    //prev->next = node_to_del->next;
                    pool_mgr->node_heap[i].prev->next = pool_mgr->node_heap[i].next;
                    //node_to_del->next->prev = prev;
                    pool_mgr->node_heap[i].next->prev = pool_mgr->node_heap[i].prev;

                }
                else
                {
                    //prev->next = NULL;
                    pool_mgr->node_heap[i].prev->next = NULL;
                }
                //node_to_del->next = NULL;
                pool_mgr->node_heap[i].next = NULL;
                //node_to_del->prev = NULL;
                pool_mgr->node_heap[i].prev = NULL;

                // change the node to add to the previous node!
                node = pool_mgr->node_heap[i].prev;
            }

            // add the resulting node to the gap index
            // check success
            _mem_add_to_gap_ix(pool_mgr,node->alloc_record.size, node);
            break;
        }
    }

    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    // allocate the segments array with size == used_nodes
    // check successful
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store()
{
    // check if necessary
    if (((float)pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR)
    {
        unsigned new_size = MEM_POOL_STORE_INIT_CAPACITY * MEM_POOL_STORE_EXPAND_FACTOR;

        pool_store = realloc(pool_store, new_size);

        if(pool_store == NULL)
        {
            return ALLOC_FAIL;
        }

        // don't forget to update capacity variables
        pool_store_capacity = new_size;

    }

    return ALLOC_OK;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr)
{
    // see above
    if(((float)pool_mgr->used_nodes / pool_mgr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR)
    {
        unsigned new_size = MEM_NODE_HEAP_INIT_CAPACITY * MEM_NODE_HEAP_EXPAND_FACTOR;

        pool_mgr->node_heap = realloc(pool_mgr->node_heap, new_size);

        if(pool_mgr->node_heap == NULL)
        {
            return ALLOC_FAIL;
        }

        pool_mgr->total_nodes = new_size;
    }

    return ALLOC_OK;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr)
{
    // see above

    if(((float)pool_mgr->used_nodes / pool_mgr->total_nodes) > MEM_GAP_IX_FILL_FACTOR)
    {
        unsigned new_size = MEM_GAP_IX_INIT_CAPACITY * MEM_GAP_IX_EXPAND_FACTOR;

        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, new_size);

        if(pool_mgr->gap_ix == NULL)
        {
            return ALLOC_FAIL;
        }

        pool_mgr->gap_ix_capacity = new_size;
    }

    return ALLOC_OK;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node)
{

    // expand the gap index, if necessary (call the function)
    alloc_status result = _mem_resize_gap_ix(pool_mgr);
    //assert(result == ALLOC_OK);
    if (result != ALLOC_OK)
    {
        return ALLOC_FAIL;
    }
    // add the entry at the end
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps += 1;

    // sort the gap index (call the function)
    result = _mem_sort_gap_ix(pool_mgr);
    //assert(result == ALLOC_OK);
    if (result != ALLOC_OK)
    {
        return ALLOC_FAIL;
    }

    // check success

    return result;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node)
{
    // find the position of the node in the gap index
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    for(int i = 0; i < pool_mgr->used_nodes; i++) {

        if (pool_mgr->gap_ix[i].node == node) {

            for(; i < pool_mgr->used_nodes; i++) {
                pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i++];
            }
        }
    }
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps -= 1;
    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr)
{
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:

    node_pt temp;

    int i = pool_mgr->pool.num_gaps - 1;
    for(; i > 0; i--)
    {
        if(pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i--].size)
        {
            temp = pool_mgr->gap_ix[i].node;

            pool_mgr->gap_ix[i].node = pool_mgr->gap_ix[i--].node;

            pool_mgr->gap_ix[i--].node = temp;
        }
        if(pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i--].size
                && &(pool_mgr->gap_ix[i].node->alloc_record.mem) <
                   &(pool_mgr->gap_ix[i--].node->alloc_record.mem))
        {
            temp = pool_mgr->gap_ix[i].node;

            pool_mgr->gap_ix[i].node = pool_mgr->gap_ix[i--].node;

            pool_mgr->gap_ix[i--].node = temp;
        }
    }
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)

    return ALLOC_OK;
}

static alloc_status _mem_invalidate_gap_ix(pool_mgr_pt pool_mgr)
{
    return ALLOC_FAIL;
}
