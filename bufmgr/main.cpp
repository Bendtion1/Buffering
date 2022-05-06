#include <iostream>
#include <stdlib.h>
//#include <stdio.h>
#include <cstring>
#include <memory>
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/buffer_exceeded_exception.h"

#define PRINT_ERROR(str) \
{ \
	std::cerr << "On Line No:" << __LINE__ << "\n"; \
	std::cerr << str << "\n"; \
	exit(1); \
}

using namespace badgerdb;

const PageId num = 100;
PageId pid[num], pageno1, pageno2, pageno3, i;
RecordId rid[num], rid2, rid3;
Page *page, *page2, *page3;
char tmpbuf[100];
BufMgr* bufMgr;
File *file1ptr, *file2ptr, *file3ptr, *file4ptr, *file5ptr;

void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void testBufMgr();

int main()
{
	//Following code shows how to you File and Page classes

  const std::string& filename = "test.db";
  // Clean up from any previous runs that crashed. 清除之前任何崩溃的运行。/ Qīngchú zhīqián rènhé bēngkuì de yùnxíng.
  try
	{
    File::remove(filename);
  }
	catch(FileNotFoundException)
	{
  }

  {
    // Create a new database file. 创建一个新的数据库文件。Chuàngjiàn yīgè xīn de shùjùkù wénjiàn.
    File new_file = File::create(filename);

    // Allocate some pages and put data on them.分配一些页面并将数据放在上面。Fēnpèi yīxiē yèmiàn bìng jiāng shùjù fàng zài shàngmiàn.
    PageId third_page_number;
    for (int i = 0; i < 5; ++i) {
      Page new_page = new_file.allocatePage();
      if (i == 3) {
        //  跟踪第三页的标识符，以便我们稍后阅读 Gēnzōng dì sān yè de biāozhì fú, yǐbiàn wǒmen shāo hòu yuèdú
        // later.
        third_page_number = new_page.page_number();
      }
      new_page.insertRecord("hello!");
      //  将页面写回文件（使用新数据）。Jiāng yèmiàn xiě huí wénjiàn (shǐyòng xīn shùjù).
      new_file.writePage(new_page);
    }

    // 遍历文件中的所有页面。 Biànlì wénjiàn zhōng de suǒyǒu yèmiàn.
    for (FileIterator iter = new_file.begin(); iter != new_file.end(); ++iter) {
      Page curr_page = (*iter);
      // 遍历页面上的所有记录 Biànlì yèmiàn shàng de suǒyǒu jìlù
      for (PageIterator page_iter = curr_page.begin();
           page_iter != curr_page.end(); ++page_iter) {
        std::cout << "Found record: " << *page_iter << " on page "
                  << curr_page.page_number() << "\n";
      }
    }

    // 检索第三页并向其中添加另一条记录。 Jiǎnsuǒ dì sān yè bìng xiàng qízhōng tiānjiā lìng yītiáo jìlù.
    Page third_page = new_file.readPage(third_page_number);
    const RecordId& rid = third_page.insertRecord("world!");
    new_file.writePage(third_page);

    //  检索我们刚刚添加到第三页的记录。Jiǎnsuǒ wǒmen gānggāng tiānjiā dào dì sān yè de jìlù.
    std::cout << "Third page has a new record: "
        << third_page.getRecord(rid) << "\n\n";
  }
  // new_file 在这里超出范围，因此文件会自动关闭。 New_file zài zhèlǐ chāochū fànwéi, yīncǐ wénjiàn huì zìdòng guānbì. This function tests buffer manager, comment this line if you don't wish to test buffer manager

  // 删除文件，因为我们已经完成了它。 Shānchú wénjiàn, yīnwèi wǒmen yǐjīng wánchéngle tā.
  File::remove(filename);

	//此函数测试缓冲区管理器，如果您不想测试缓冲区管理器，请注释此行 Cǐ hánshù cèshì huǎnchōng qū guǎnlǐ qì, rúguǒ nín bùxiǎng cèshì huǎnchōng qū guǎnlǐ qì, qǐng zhùshì cǐ xíng
	testBufMgr();
}

void testBufMgr()
{
	// create buffer manager
	bufMgr = new BufMgr(num);

	// create dummy files 创建虚拟文件 Chuàngjiàn xūnǐ wénjiàn
  const std::string& filename1 = "test.1";
  const std::string& filename2 = "test.2";
  const std::string& filename3 = "test.3";
  const std::string& filename4 = "test.4";
  const std::string& filename5 = "test.5";

  try
	{
    File::remove(filename1);
    File::remove(filename2);
    File::remove(filename3);
    File::remove(filename4);
    File::remove(filename5);
  }
	catch(FileNotFoundException e)
	{
  }

	File file1 = File::create(filename1);
	File file2 = File::create(filename2);
	File file3 = File::create(filename3);
	File file4 = File::create(filename4);
	File file5 = File::create(filename5);

	file1ptr = &file1;
	file2ptr = &file2;
	file3ptr = &file3;
	file4ptr = &file4;
	file5ptr = &file5;

	//测试缓冲区管理器 Cèshì huǎnchōng qū guǎnlǐ qì
	//评论您现在不想运行的测试。 测试依赖于它们之前的测试。 因此，它们必须按以下顺序运行。 Pínglùn nín xiànzài bùxiǎng yùnxíng de cèshì. Cèshì yīlài yú tāmen zhīqián de cèshì. Yīncǐ, tāmen bìxū àn yǐxià shùnxù yùnxíng.
	//评论一个特定的测试需要评论它后面的所有测试，否则这些测试将失败。 Pínglùn yīgè tèdìng de cèshì xūyào pínglùn tā hòumiàn de suǒyǒu cèshì, fǒuzé zhèxiē cèshì jiāng shībài.
	test1();
	test2();
	test3();
	test4();
	test5();
	test6();

	//在删除文件之前关闭文件 Zài shānchú wénjiàn zhīqián guānbì wénjiàn
	file1.~File();
	file2.~File();
	file3.~File();
	file4.~File();
	file5.~File();

	//删除文件 Shānchú wénjiàn
	File::remove(filename1);
	File::remove(filename2);
	File::remove(filename3);
	File::remove(filename4);
	File::remove(filename5);

	delete bufMgr;

	std::cout << "\n" << "Passed all tests." << "\n";
}

void test1()
{
	//在文件中分配页面...  Zài wénjiàn zhōng fēnpèi yèmiàn...
	for (i = 0; i < num; i++)
	{
		bufMgr->allocPage(file1ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
		bufMgr->unPinPage(file1ptr, pid[i], true);
	}

	//回读页面... Huí dú yèmiàn...
	for (i = 0; i < num; i++)
	{
		bufMgr->readPage(file1ptr, pid[i], page);
		sprintf((char*)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}
	std::cout<< "Test 1 passed" << "\n";
}

void test2()
{
	//写和读回多个文件 Xiě hé dú huí duō gè wénjiàn
	//页码和值应该匹配 Yèmǎ hé zhí yīnggāi pǐpèi

	for (i = 0; i < num/3; i++)
	{
		bufMgr->allocPage(file2ptr, pageno2, page2);
		sprintf((char*)tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		rid2 = page2->insertRecord(tmpbuf);

		int index = random() % num;
    pageno1 = pid[index];
		bufMgr->readPage(file1ptr, pageno1, page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pageno1, (float)pageno1);
		if(strncmp(page->getRecord(rid[index]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->allocPage(file3ptr, pageno3, page3);
		sprintf((char*)tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		rid3 = page3->insertRecord(tmpbuf);

		bufMgr->readPage(file2ptr, pageno2, page2);
		sprintf((char*)&tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		if(strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->readPage(file3ptr, pageno3, page3);
		sprintf((char*)&tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		if(strncmp(page3->getRecord(rid3).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->unPinPage(file1ptr, pageno1, false);
	}

	for (i = 0; i < num/3; i++) {
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
	}

	std::cout << "Test 2 passed" << "\n";
}

void test3()
{
	try
	{
		bufMgr->readPage(file4ptr, 1, page);
		PRINT_ERROR("ERROR :: File4 should not exist. Exception should have been thrown before execution reaches this point.");
	}
	catch(InvalidPageException e)
	{
	}

	std::cout << "Test 3 passed" << "\n";
}

void test4()
{
	bufMgr->allocPage(file4ptr, i, page);
	bufMgr->unPinPage(file4ptr, i, true);
	try
	{
		bufMgr->unPinPage(file4ptr, i, false);
		PRINT_ERROR("ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
	}
	catch(PageNotPinnedException e)
	{
	}

	std::cout << "Test 4 passed" << "\n";
}

void test5()
{
	for (i = 0; i < num; i++) {
		bufMgr->allocPage(file5ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.5 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
	}

	PageId tmp;
	try
	{
		bufMgr->allocPage(file5ptr, tmp, page);
		PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution reaches this point.");
	}
	catch(BufferExceededException e)
	{
	}

	std::cout << "Test 5 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file5ptr, i, true);
}

void test6()
{
	//刷新页面仍然固定的文件。 应该产生错误 Shuāxīn yèmiàn réngrán gùdìng de wénjiàn. Yīnggāi chǎnshēng cuòwù
	for (i = 1; i <= num; i++) {
		bufMgr->readPage(file1ptr, i, page);
	}

	try
	{
		bufMgr->flushFile(file1ptr);
		PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution reaches this point.");
	}
	catch(PagePinnedException e)
	{
	}

	std::cout << "Test 6 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file1ptr, i, true);

	bufMgr->flushFile(file1ptr);
}
