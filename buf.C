#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{

    int iterations = 0;
    bool found = false;

    while (iterations < 2 * numBufs) {
        if (bufTable[clockHand].pinCnt == 0) {
            // Unpinned frame found
            if (bufTable[clockHand].refbit == false) {
                // Frame is unpinned and has refbit = false, so it's suitable for replacement
                found = true;
                break;
            } else {
                // Clear the refbit to give it a "second chance" on the next pass
                bufTable[clockHand].refbit = false;
            }
        }

        advanceClock();
        iterations++;
    }

    // Check if we have gone through the buffer twice without finding a suitable frame
    if (!found) {
        return BUFFEREXCEEDED;
    }

    if(bufTable[clockHand].dirty==true){
        Status status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
        if(status!= OK){
            return UNIXERR;
        }
    }
    if(bufTable[clockHand].valid){
        hashTable->remove(bufTable[clockHand].file,bufTable[clockHand].pageNo);
    }
    bufTable[clockHand].Clear();
    frame = clockHand;
    return OK;
}



const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{

    // check if page in buffer pool using hashtable, lookup()
    int frameNo = 0;
    Status lookupStatus = hashTable->lookup(file, PageNo, frameNo);

    // Case 1: Not in buffer pool, call allocBuf(), then call the method file->readPage() which reads page from disk to buffer pool
    //  insert page in hashmap, call Set() on page, return a pointer to the frame containing the page via the page parameter
    if (lookupStatus == HASHNOTFOUND) {
        Status allocBufStatus = allocBuf(frameNo);

        if (allocBufStatus != OK) {
            return allocBufStatus;
        }

        Status readPageStatus = file->readPage(PageNo, bufPool+frameNo);

        if (readPageStatus != OK) {
            return readPageStatus;
        }

        Status hashTableInsertStatus = hashTable->insert(file, PageNo, frameNo);

        if (hashTableInsertStatus != OK) {
            return hashTableInsertStatus;
        }
        BufDesc* currentBuf = &bufTable[frameNo];
        currentBuf->Set(file, PageNo);
        page = &bufPool[frameNo];
    }

    // Case 2: Page in buffer pool, set appropriate refbit, increment the pinCnt, return a pointer to the frame containing the page via the page parameter
    else {
        BufDesc* currentBuf = &bufTable[frameNo];
        currentBuf->pinCnt++;
        currentBuf->refbit = true;
        page = &bufPool[frameNo];
    }

    // Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred.
        return OK;
}


/**
 * THis method unpins the page, it decrements the pinCnt for specified page
 * input - ptr to file, its pageNo and whether it is dirty or not
 * return - status of the unpin
 */
const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty)
{
    // placeholder for frameNo
    int frameNo = -1;
    // return status
    Status retStatus = hashTable->lookup(file, PageNo, frameNo);
    // check if the lookup function returned OK, if so continue
    if (retStatus == OK){
        // check to see if frame is not pinned
        if (bufTable[frameNo].pinCnt == 0){
            return PAGENOTPINNED;
        }
        // Decrements the pinCnt of the frame containing (file, PageNo)
        bufTable[frameNo].pinCnt -= 1;
        // if dirty == true, sets the dirty bit
        if (dirty){
            bufTable[frameNo].dirty = true;
        }
    }
    return retStatus;
}

/**
 * Allocates a page in the specified file and pins it in the buffer pool.
 * 
 * @param file   - pointer to the file object in which to allocate the page.
 * @param pageNo - reference to an integer that will hold the allocated page number.
 * @param page   - reference to a pointer that will point to the allocated page data.
 * 
 * @return Status - OK if successful, UNIXERR if a Unix error occurred, 
 *                  BUFFEREXCEEDED if all buffer frames are pinned, 
 *                  HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{

    // allocate empty page in specified file by using file->allocatePage(pageNo)
    Status retStatus = file->allocatePage(pageNo);
    // allocBuf() is called to obtain a buffer pool frame, set page as the reutrn of this
    if (retStatus == OK) {
        int frameNo = -1;
        retStatus = allocBuf(frameNo);
        if (retStatus == OK) {
            // insert into hashtable, call Set() on the frame in bufTable
            retStatus = hashTable->insert(file, pageNo, frameNo);
            if (retStatus == OK) {
                bufTable[frameNo].Set(file, pageNo);
            }
            page = bufPool+frameNo;
        }
    }
    
    return retStatus;
}


const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
