/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // 分配 the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	for (unsigned int i = 0; i < numBufs; i++) {
		if (bufDescTable[i].dirty && bufDescTable[i].valid) {
			bufDescTable[i].file->writePage(bufPool[i]);
			bufDescTable[i].dirty = false;
		}
	}

	// 解除分配 Jiěchú fēnpèi
	delete[] bufDescTable;
	delete[] bufPool;
	delete hashTable;
}

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
  bool openFrameFound = false;
  int counter = -1;
  while (counter < (int)numBufs && openFrameFound == false)
  {
    counter++;
    advanceClock();
    if (bufDescTable[clockHand].valid == true)
    {
      if (bufDescTable[clockHand].refbit == true)
      {
        bufDescTable[clockHand].refbit = false;
        continue; // 提前时钟再试一次 Tíqián shízhōng zài shì yīcì
      }
      else if (bufDescTable[clockHand].pinCnt > 0)
      {
        continue; // 提前时钟再试一次 Tíqián shízhōng zài shì yīcì
      }
      else if (bufDescTable[clockHand].refbit == false && bufDescTable[clockHand].pinCnt == 0)
      {
        // Use the frame
        frame = clockHand;

        if (bufDescTable[clockHand].dirty)
        {
          // 刷新页面到磁盘 Shuāxīn yèmiàn dào cípán
          bufDescTable[clockHand].file->writePage(bufPool[frame]);
        }
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
      }
      openFrameFound = true;
      bufDescTable[clockHand].Clear();
    }
    else
    {
      // Use the frame
      frame = clockHand;
      openFrameFound = true;
      bufDescTable[clockHand].Clear();
    }
  }
  if (openFrameFound == false)
  {
    throw BufferExceededException();
  }
}


void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{

  FrameId frameNo; // 填写 tianxie  hashTable->lookup
  // c检查页面是否在hash table Jiǎnchá yèmiàn shìfǒu zài
  try
  {
    // 检查页面是否在 hashTable
    hashTable->lookup(file, pageNo, frameNo);
    // Case 2

    // 设置适当的 refbit
    bufDescTable[frameNo].refbit = true;
    // 增加页面的 pinCnt Zēngjiā yèmiàn de pinCnt
    bufDescTable[frameNo].pinCnt++;
  }
  catch (HashNotFoundException& e)
  {
    // Case 1
    // 调用 allocBuf() 分配一个缓冲帧 Diàoyòng allocBuf() fēnpèi yīgè huǎnchōng zhèng
    allocBuf(frameNo);

    // 调用方法file->readPage()读取页面 Diàoyòng fāngfǎ file->readPage() dòu qǔ yèmiàn
    // 从磁盘进入缓冲池帧。 Cóng cípán jìnrù huǎnchōng chí zhèng.
    Page newPage = file->readPage(pageNo);
    // Add newPage to bufPool at the index 'frameNo'
    bufPool[frameNo] = newPage;

    // 接下来，将页面插入哈希表
    hashTable->insert(file, pageNo, frameNo);

    // 最后，在框架上调用 Set() 以正确设置它 Zuìhòu, zài kuàngjià shàng diàoyòng Set() yǐ zhèngquè shèzhì tā
    bufDescTable[frameNo].Set(file, pageNo);
  }
    // 通过 page 参数返回指向包含页面的帧的指针。Tōngguò page cānshù fǎnhuí zhǐxiàng bāohán yèmiàn de zhèng de zhǐzhēn.
    // the page via the page parameter.
    page = &bufPool[frameNo];
}


void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
  try
  {
    // 检查页面是否在哈希表中 Jiǎnchá yèmiàn shìfǒu zài hā xī biǎo zhōng
    FrameId frameNum; // to be replaced by the hashTable->lookup
    hashTable->lookup(file, pageNo, frameNum);

    if (bufDescTable[frameNum].pinCnt == 0)
    {
      // Throws PAGENOTPINNED if the pin count is already 0
      throw PageNotPinnedException("", pageNo, frameNum);
    }
    else if (bufDescTable[frameNum].pinCnt > 0)
    {
      // 减少帧的 pinCnt
      --bufDescTable[frameNum].pinCnt;
    }
    if (dirty)
    {
      // if dirty == true, sets 放 the dirty bit
      bufDescTable[frameNum].dirty = true;
    }
  }
  catch (HashNotFoundException &e)
  {
    //如果在哈希表查找中找不到页面，则不执行任何操作。 Rúguǒ zài hā xī biǎo cházhǎo zhōng zhǎo bù dào yèmiàn, zé bù zhíxíng rènhé cāozuò.
    return;
  }
}

void BufMgr::flushFile(const File *file)
{
  //扫描 bufTable 以查找属于该文件的页面（BufDesc 框架：bufDescTable）
  //Sǎomiáo bufTable yǐ cházhǎo shǔyú gāi wénjiàn de yèmiàn (BufDesc kuàngjià:BufDescTable)
  for (u_int32_t i = 0; i < numBufs; i++)
  {
    //如果页面是脏的，调用 file->writepage() 然后将脏位设置为 false
    if (bufDescTable[i].file == file)
    {
      if (bufDescTable[i].valid == false)
      {
        throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      // 如果文件的某些页面被固定，则引发 PagePinnedException。 Rúguǒ wénjiàn de mǒu xiē yèmiàn bèi gùdìng, zé yǐnfā PagePinnedException.
      if (bufDescTable[i].pinCnt > 0)
      {
        throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
      }
      if (bufDescTable[i].dirty)
      {
        // 如果页面是脏的，调用 file->writePage() 将页面刷新到磁盘 Rúguǒ yèmiàn shì zàng de, diàoyòng file->writePage() jiāng yèmiàn shuāxīn dào cípán,
        // 然后将页面的脏位设置为 false ránhòu jiāng yèmiàn de zàng wèi shèzhì wèi false

        // file->writePage(bufDescTable[i].pageNo, Page)
        // bufDescTable[i].file->writePage(bufDescTable[i].pageNo, bufDescTable[i].pageNo);
        bufDescTable[i].file->writePage(bufPool[i]);
        bufDescTable[i].dirty = false;
      }
      // 如果遇到属于该文件的无效页面，则抛出 BadBufferException Rúguǒ yù dào shǔyú gāi wénjiàn de wúxiào yèmiàn, zé pāo chū BadBufferException

      if (Page::INVALID_NUMBER == bufDescTable[i].pageNo)
      {
        throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      // 从哈希表中删除页面（页面是干净的还是脏的）Cóng hā xī biǎo zhōng shānchú yèmiàn (yèmiàn shì gānjìng de háishì zàng de)

      hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);

      // 为页框调用 BufDesc 的 Clear() 方法 Wèi yè kuāng diàoyòng BufDesc de Clear() fāngfǎ
      bufDescTable[i].Clear();
    }
  }
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
  // 该方法的第一步是通过调用file->allocatePage()方法在指定文件中分配一个空页
  // Gāi fāngfǎ de dì yī bù shì tōngguò diàoyòng file->allocatePage() fāngfǎ zài zhǐdìng wénjiàn zhōng fēnpèi yīgè kōng yè
  // 此方法将返回一个新分配的页面。 Cǐ fāngfǎ jiāng fǎnhuí yīgè xīn fēnpèi de yèmiàn.
  Page newPage = file->allocatePage();

  // 然后调用 allocBuf() 获取缓冲池帧 Ránhòu diàoyòng allocBuf() huòqǔ huǎnchōng chí zhèng.
  FrameId newFrameId;
  allocBuf(newFrameId);
  // add newPage to bufPool based on newFrameId index
  bufPool[newFrameId] = newPage;

  // 该方法通过 pageNo 参数将新分配的页面的页码返回给调用者，
  // 并通过 page 参数返回指向为该页面分配的缓冲区帧的指针。
  // Gāi fāngfǎ tōngguò pageNo cānshù jiāng xīn fēnpèi de yèmiàn de yèmǎ fǎnhuí gěi diàoyòng zhě,
  // bìng tōngguò page cānshù fǎnhuí zhǐxiàng wèi gāi yèmiàn fēnpèi de huǎnchōng qū zhèng
  page = &bufPool[newFrameId];
  pageNo = newPage.page_number();

  // 接下来，将一个条目插入到哈希表中，并在框架上调用 Set() 以正确设置它
  // Jiē xiàlái, jiāng yīgè tiáomù chārù dào hā xī biǎo zhōng, bìng zài kuàngjià shàng diàoyòng Set() yǐ zhèngquè shèzhì tā
  hashTable->insert(file, pageNo, newFrameId);
  bufDescTable[newFrameId].Set(file, pageNo);
}

void BufMgr::disposePage(File* file, const PageId PageNo) {
  try {
    FrameId frameNo; // blank frameNo to use for search
    hashTable->lookup(file, PageNo, frameNo);
    hashTable->remove(file, PageNo);
    bufDescTable[frameNo].Clear();
  } catch (HashNotFoundException& e) {
    // 没找到，继续。。。
   }
  }

  // 删除页面
  file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
