//
// Created by jinho on 3/5/2019.
//todo:make range search faster


#pragma once
#ifndef TTRS_BACKEND_B_TREE_H
#define TTRS_BACKEND_B_TREE_H

#include <queue>
#include "BulkLoading.h"
#include "page/header_page.h"
#include "page/table_page.h"
#include "MemoryController.h"
#include "utility.hpp"
#include "exceptions.hpp"
#include "string.hpp"
#include "vector.hpp"
#include "buffer/buffer_pool_manager.h"
#include <iostream>

#include "concurrency/transaction_manager.h"
template <class T>
struct Comparator{
    virtual bool operator()(const T& t1,const T& t2)const=0;
};

namespace sjtu {
    extern DiskManager recorddisk;
    extern DiskManager userdisk;
    extern LogManager logManager;
    extern BufferPoolManager recordbpm;
    extern TransactionManager transactionManager;

    //限制Value 只能是RID
    template<class Key , class Value , class Compare=std::less<Key>>
    class Bptree {
        const static unsigned maxEntryNum = (PAGE_SIZE - 40) / sizeof(pair<Key , Value>);
        const static unsigned maxSonNum = (PAGE_SIZE - 40) / (sizeof(Key) + sizeof(page_id_t));
//        const static unsigned maxEntryNum = 4;
//        const static unsigned maxSonNum = 4;



        struct Node {
            int32_t isLeaf;
            lsn_t lsn=INVALID_LSN;
            page_id_t parent=INVALID_PAGE_ID;
            page_id_t page_id=INVALID_PAGE_ID;
            page_id_t right=INVALID_PAGE_ID;
            Bptree *baseTree;
            Node (bool isleaf , page_id_t parent , page_id_t address , Bptree *basetree , page_id_t left , page_id_t
            right) :
                    isLeaf(isleaf) , parent(parent) , page_id(address) , baseTree(basetree)  ,
                    right(right) {}
            inline lsn_t getLSN(){ return lsn;}
            inline void setLSN(lsn_t lsn){this->lsn=lsn;}
            inline void settype(bool isleaf){this->isLeaf=isleaf;}
            inline void setPageId(page_id_t pageId){this->page_id=pageId;}
            void init(page_id_t  pageId,page_id_t parent,page_id_t right,bool isleaf,Transaction* txn,Bptree* basetree){
                page_id=pageId;
                this->baseTree=basetree;
                this->parent=parent;
                this->right=right;
                this->isLeaf=isleaf;
                if(ENABLE_LOGGING) {
                    LogRecord lg(txn->GetTransactionId() , txn->GetPrevLSN() , LogRecordType::NEWBPTREEPAGE,pageId,
                            parent,right,isleaf);
                    lsn = logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(lsn);
                }
            }
        };

        struct InnerNode :  public Node {
            array<page_id_t , maxSonNum> sons;
            array<Key , maxSonNum> keywords;

            InnerNode (page_id_t parent , page_id_t address , Bptree *basetree , page_id_t left , page_id_t right)
                    : Node
                              (false ,
                               parent , address ,
                               basetree , left , right) {}

            InnerNode (Bptree *basetree) : Node(false , -1 , -1 , basetree , -1 , -1) {};

            pair<page_id_t , int> findSon (const Key &key) {
                int start = -1 , end = sons.size() - 1;
                while (start < end - 1) {
                    int mid = (start + end) / 2;
                    if (!Compare()(key , keywords[mid])) {
                        start = mid;
                    } else {
                        end = mid;
                    }
                }
                return pair<page_id_t , int>(sons[end] , end);
            }


            void insertSon (const Key &key , page_id_t son , Transaction *txn) {
                int start = -1 , end = sons.size() - 1;
                while (start < end - 1) {
                    int mid = (start + end) / 2;
                    if (!Compare()(key , keywords[mid])) {
                        start = mid;
                    } else {
                        end = mid;
                    }
                }
                if (end != -1) {
                    keywords.insert(end , key);
                }
                sons.insert(end + 1 , son);
                if(ENABLE_LOGGING) {
                    LogRecord lg(txn->GetTransactionId() , txn->GetPrevLSN() , LogRecordType::INSERTPTR ,
                                 this->page_id , son);
                    lsn_t curlsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(curlsn);
                    Tuple key_tuple;
                    key_tuple.DeserializeFrom(reinterpret_cast<const char*>(&key),sizeof(key));
                    LogRecord lg2(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::INSERTKEY,this->page_id,
                            key_tuple);
                    curlsn=logManager.AppendLogRecord(lg2);
                    txn->SetPrevLSN(curlsn);
                    this->setLSN(curlsn);
                    txn->GetRecordSet()->push_back(lg);
                    txn->GetRecordSet()->push_back(lg2);
                }
            }

            /**
             *
             * @param pos 在关键词数组的pos位置分裂（上推key[pos])
             * @return
             */
            void split (int pos , Transaction *txn) {
                int newpageid;
                Page* newpage= recordbpm.NewPage(newpageid , 0);
                auto *addednode = reinterpret_cast<InnerNode*>(newpage->GetData());
                addednode->init(newpageid,this->parent,this->right,false,txn,this->baseTree);
                if(ENABLE_LOGGING) {
                    LogRecord lg(txn->GetTransactionId() , txn->GetPrevLSN() ,LogRecordType::MOVE
                            ,this->page_id,pos+1,RID(addednode->page_id,0),sons.size()-pos-1);
                    lsn_t curlsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(curlsn);
                    this->setLSN(curlsn);
                    txn->GetRecordSet()->push_back(lg);
                }
                for (int i = pos + 1; i < sons.size(); i++) {
                    addednode->sons.push_back(sons[i]);
                }
                for (int i = pos + 1; i < sons.size() - 1; i++) {
                    addednode->keywords.push_back(keywords[i]);
                }
                auto keyup = keywords[pos];
                sons.setsize(pos + 1);
                keywords.setsize(pos);
                if (this->parent != -1) {
                    recordbpm.UnpinPage(addednode->page_id , true , 0);
                    Page* parentPage= recordbpm.FetchPage(this->parent , 0);
                    InnerNode* parentnode= reinterpret_cast<InnerNode*>(parentPage->GetData());
                    parentnode->baseTree=this->baseTree;
                    parentnode->insertSon(keyup , addednode->page_id , txn);
                    if (parentnode->sons.size() >= maxSonNum) {
                        parentnode->split((maxSonNum - 1) / 2 , txn);
                    }
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                } else {
                    int newrootpageid;
                    Page* newrootpage= recordbpm.NewPage(newrootpageid , 0);
                    auto* newroot= reinterpret_cast<InnerNode*>(newrootpage->GetData());
                    newroot->init(newrootpageid,-1,-1,0,txn,this->baseTree);
                    newroot->insertSon(this->keywords[0],this->page_id,txn);
                    newroot->insertSon(keyup,addednode->page_id,txn);
                    this->baseTree->root = newroot->page_id;
                    this->baseTree->updateRootPageId(false , txn);
                    recordbpm.UnpinPage(newroot->page_id , true , 0);
                }
                recordbpm.UnpinPage(addednode->page_id , true , 0);


            }

            /**
             *
             * @param pos the position of the deleted son
             *
             */
            void deleteSon (int pos , Transaction *txn) {
                if (ENABLE_LOGGING) {
                    LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::DELETEPTR,this->page_id,sons[pos]);
                    lsn_t curlsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(curlsn);
                    this->setLSN(curlsn);
                    txn->GetRecordSet()->push_back(lg);
                }
                sons.erase(pos);
                if (pos == 0) {
                    keywords.erase(0);
                } else {
                    keywords.erase(pos - 1);
                }
                if (sons.size() >= (maxSonNum + 1) / 2 || this->baseTree->root==this->page_id) {
                    if (this->keywords.size() == 0) {
                        this->baseTree->root = sons[0];
                        this->baseTree->updateRootPageId(false , txn);
                        recordbpm.UnpinPage(this->page_id , false , 0);
                        recordbpm.DeletePage(this->page_id , 0);
                    }
                    return;
                }
                Page* parentPage= recordbpm.FetchPage(this->parent , 0);
                auto *parentnode = reinterpret_cast<InnerNode*>(parentPage->GetData());
                parentnode->baseTree=this->baseTree;
                InnerNode *leftcousin = nullptr , *rightcousin = nullptr;
                auto posInParent = parentnode->findSon(keywords[0]).second;
                if (posInParent != parentnode->sons.size() - 1) {
                    Page* rightcousinPage= recordbpm.FetchPage(parentnode->sons[posInParent + 1] , 0);
                    rightcousin = reinterpret_cast<InnerNode*>(rightcousinPage->GetData());
                    rightcousin->baseTree=this->baseTree;
                }
                if (posInParent != 0) {
                    Page* leftcousinPage= recordbpm.FetchPage(parentnode->sons[posInParent - 1] , 0);
                    leftcousin = reinterpret_cast<InnerNode*>(leftcousinPage->GetData());
                    leftcousin->baseTree=this->baseTree;
                }
                if (rightcousin && rightcousin->sons.size() > (maxSonNum) / 2) {
                    auto right_first_son=rightcousin->sons[0];
                    auto right_first_key=rightcousin->keywords[0];
                    insertSon(parentnode->keywords[posInParent],right_first_son,txn);
                    rightcousin->deleteSon(0,txn);
                    if (ENABLE_LOGGING) {
                        Tuple old_key_tuple,new_key_tuple;
                        old_key_tuple.DeserializeFrom(reinterpret_cast<const char*>
                        (&parentnode->keywords[posInParent]),sizeof(Key));
                        new_key_tuple.DeserializeFrom(reinterpret_cast<char*>(&right_first_key),sizeof(Key));
                        LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                     LogRecordType::UPDATEKEY,parentnode->page_id,
                                     old_key_tuple,new_key_tuple);
                        lsn_t curlsn=logManager.AppendLogRecord(lg);
                        txn->SetPrevLSN(curlsn);
                        this->setLSN(curlsn);
                        txn->GetRecordSet()->push_back(lg);
                    }
                    parentnode->keywords[posInParent] = right_first_key;
                    recordbpm.UnpinPage(rightcousin->page_id , true , 0);
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                    if(leftcousin)
                        recordbpm.UnpinPage(leftcousin->page_id , false , 0);
                } else if (leftcousin && leftcousin->sons.size() > (maxSonNum) / 2) {
                    auto left_last_son=leftcousin->sons.back();
                    auto left_last_key=leftcousin->keywords.back();
                    insertSon(parentnode->keywords[posInParent - 1],left_last_son,txn);
                    leftcousin->deleteSon(leftcousin->sons.size()-1,txn);
                    if (ENABLE_LOGGING) {
                        Tuple old_key_tuple,new_key_tuple;
                        old_key_tuple.DeserializeFrom(reinterpret_cast<const char*>
                                                      (&parentnode->keywords[posInParent-1]),sizeof(Key));
                        new_key_tuple.DeserializeFrom(reinterpret_cast<char*>(&left_last_key),sizeof(Key));
                        LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                     LogRecordType::UPDATEKEY,parentnode->page_id,
                                     old_key_tuple,new_key_tuple);
                        lsn_t curlsn=logManager.AppendLogRecord(lg);
                        txn->SetPrevLSN(curlsn);
                        this->setLSN(curlsn);
                        txn->GetRecordSet()->push_back(lg);
                    }
                    parentnode->keywords[posInParent - 1] = left_last_key;
                    recordbpm.UnpinPage(leftcousin->page_id , true , 0);
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                    if(rightcousin)
                        recordbpm.UnpinPage(rightcousin->page_id , false , 0);
                } else {
                    if (rightcousin) {
                        if (ENABLE_LOGGING) {
                            Tuple key_tuple;
                            key_tuple.DeserializeFrom(reinterpret_cast<char*>(&keywords[posInParent]),sizeof(Key));
                            LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::INSERTKEY,this->page_id,key_tuple);
                            lsn_t curlsn=logManager.AppendLogRecord(lg);
                            txn->SetPrevLSN(curlsn);
                            LogRecord lg2(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::MOVE,rightcousin->page_id,0,RID(this->page_id,sons.size()),rightcousin->sons.size());
                            curlsn=logManager.AppendLogRecord(lg2);
                            txn->SetPrevLSN(curlsn);
                            this->setLSN(curlsn);
                            txn->GetRecordSet()->push_back(lg);
                            txn->GetRecordSet()->push_back(lg2);
                        }
                        this->sons.merge(rightcousin->sons);
                        this->keywords.push_back(parentnode->keywords[posInParent]);
                        this->keywords.merge(rightcousin->keywords);
                        recordbpm.UnpinPage(rightcousin->page_id , false , 0);
                        recordbpm.DeletePage(rightcousin->page_id , 0);
                        if(leftcousin)
                            recordbpm.UnpinPage(leftcousin->page_id , false , 0);
                        parentnode->deleteSon(posInParent + 1 , txn);
                    } else if (leftcousin) {
                        if (ENABLE_LOGGING) {
                            Tuple key_tuple;
                            key_tuple.DeserializeFrom(reinterpret_cast<char*>(&keywords[posInParent-1]),sizeof(Key));
                            LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::INSERTKEY,leftcousin->page_id,key_tuple);
                            lsn_t curlsn=logManager.AppendLogRecord(lg);
                            txn->SetPrevLSN(curlsn);
                            LogRecord lg2(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::MOVE,this->page_id,0,RID(leftcousin->page_id,leftcousin->sons.size()),sons.size());
                            curlsn=logManager.AppendLogRecord(lg2);
                            txn->SetPrevLSN(curlsn);
                            this->setLSN(curlsn);
                            txn->GetRecordSet()->push_back(lg);
                            txn->GetRecordSet()->push_back(lg2);
                        }
                        leftcousin->sons.merge(this->sons);
                        leftcousin->keywords.push_back(parentnode->keywords[posInParent - 1]);
                        leftcousin->keywords.merge(this->keywords);
                        recordbpm.UnpinPage(leftcousin->page_id , true , 0);
                        recordbpm.UnpinPage(this->page_id , false , 0);
                        recordbpm.DeletePage(this->page_id , 0);
                        parentnode->deleteSon(posInParent , txn);
                    }
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                }
            }
        };

        struct LeafNode : public Node {

            array<pair<Key , Value> , maxEntryNum> entry;

            LeafNode (Bptree *basetree) : Node(true , -1 , -1 , basetree , -1 , -1) {};

            LeafNode (page_id_t parent , page_id_t address , Bptree *basetree , page_id_t left , page_id_t right) : Node
                                                                                                                            (true ,
                                                                                                                             parent ,
                                                                                                                             address ,
                                                                                                                             basetree ,
                                                                                                                             left ,
                                                                                                                             right) {}

            /**
             *
             * @param key
             * @param success
             * @return
             * return the value corresponding to the key;
             *
             */

            pair<Value , int> findData (const Key &key,bool & success) noexcept(false) {
                int start = 0 , end = entry.size();
                while (start < end) {
                    int mid = (start + end) / 2;
                    if (!Compare()(entry[mid].first , key) && !Compare()(key , entry[mid].first)) {
                        success=true;
                        return pair<Value  , int>(entry[mid].second , mid);
                    } else if (Compare()(entry[mid].first , key)) {
                        start = mid + 1;
                    } else {
                        end = mid;
                    }
                }
                success=false;
                return pair<Value,int>(Value(),end);
            }


            /**
             *
             * @param pos 把节点在pos分裂 [0,entry.size())--->[0,pos),[pos,entry.size())
             */
            void split (int pos , Transaction *txn) {
                Page* addednodePage,*parentnodePage;
                page_id_t addednodepageid;
                addednodePage= recordbpm.NewPage(addednodepageid , 0);
                auto * addednode= reinterpret_cast<LeafNode*>(addednodePage->GetData());
                addednode->init(addednodepageid,this->parent,this->right,true,txn,this->baseTree);
                parentnodePage= recordbpm.FetchPage(this->parent , 0);
                auto *parentnode = reinterpret_cast<InnerNode*>(parentnodePage->GetData());
                if (ENABLE_LOGGING) {
                    LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::MOVE,this->page_id,pos,RID(addednode->page_id,0),entry.size()-pos);
                    lsn_t curlsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(curlsn);
                    LogRecord lg2(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::UPDATEPTR,this->page_id,
                            this->right,addednode->page_id);
                    curlsn=logManager.AppendLogRecord(lg2);
                    txn->SetPrevLSN(curlsn);
                    this->setLSN(curlsn);
                    txn->GetRecordSet()->push_back(lg);
                    txn->GetRecordSet()->push_back(lg2);
                }
                this->right = addednode->page_id;
                for (int i = pos; i < entry.size(); i++) {
                    addednode->entry.push_back(entry[i]);
                }
                auto keyup = entry[pos].first;
                entry.setsize(pos);

                if (this->parent != -1) {
                    parentnode->baseTree=this->baseTree;
                    parentnode->insertSon(keyup , addednode->page_id , txn);
                    recordbpm.UnpinPage(addednode->page_id , true , 0);

                    if (parentnode->sons.size() >= maxSonNum) {
                        parentnode->split((maxSonNum - 1) / 2 , txn);
                    }
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                } else {
                    page_id_t newrootpageid;
                    Page* newrootPage= recordbpm.NewPage(newrootpageid , 0);
                    auto* newroot= reinterpret_cast<InnerNode*>(newrootPage->GetData());
                    newroot->init(newrootpageid,-1,-1,false,txn,this->baseTree);
                    newroot->insertSon(entry[0].first,this->page_id,txn);
                    newroot->insertSon(keyup,addednode->page_id,txn);
                    recordbpm.UnpinPage(addednode->page_id , true , 0);
                    this->baseTree->root = newroot->page_id;
                    this->baseTree->updateRootPageId(false , txn);
                    recordbpm.UnpinPage(newroot->page_id , true , 0);
                }

            }

            /**
             * if you the key has been in the node,it will change the value.
             * else a new entry will be inserted
             * @param entry1
             */
            void insertData (const pair <Key , Value> &entry1 , Transaction *txn) {
                int start = -1 , end = this->entry.size();
                while (start < end - 1) {
                    int mid = (start + end) / 2;
                    if (Compare()((this->entry[mid]).first , entry1.first)) {
                        start = mid;
                    } else if (Compare()(entry1.first , (this->entry[mid]).first)) {
                        end = mid;
                    } else {

                        if (ENABLE_LOGGING) {
                            Tuple new_key_val_tuple,old_key_val_tuple;
                            new_key_val_tuple.DeserializeFrom(reinterpret_cast<const char*>(&entry1),sizeof(entry1));
                            old_key_val_tuple.DeserializeFrom(reinterpret_cast<const char*>(&entry[mid]),sizeof
                            (entry1));
                            LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN()
                                    ,LogRecordType::UPDATEKEY,this->page_id,old_key_val_tuple,
                                    new_key_val_tuple);
                            lsn_t cur_lsn=logManager.AppendLogRecord(lg);
                            txn->SetPrevLSN(cur_lsn);
                            this->setLSN(cur_lsn);
                            txn->GetRecordSet()->push_back(lg);
                        }
                        this->entry[mid] = entry1;
                        return;
                    }
                }
                if (ENABLE_LOGGING) {
                    Tuple key_val_tuple;
                    key_val_tuple.DeserializeFrom(reinterpret_cast<const char*>(&entry1),sizeof(entry1));
                    LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::INSERTKEY,this->page_id,key_val_tuple);
                    lsn_t cur_lsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(cur_lsn);
                    this->setLSN(cur_lsn);
                    txn->GetRecordSet()->push_back(lg);
                }
                this->entry.insert(end , entry1);

            }

            bool addData (const pair <Key , Value> &entry1 , Transaction *txn) {
                int start = -1 , end = this->entry.size();
                while (start < end - 1) {
                    int mid = (start + end) / 2;
                    if (Compare()((this->entry[mid]).first , entry1.first)) {
                        start = mid;
                    } else if (Compare()(entry1.first , (this->entry[mid]).first)) {
                        end = mid;
                    } else {
                        return false;
                    }
                }
                this->entry.insert(end , entry1);
                if (ENABLE_LOGGING) {
                    Tuple key_val_tuple;
                    key_val_tuple.DeserializeFrom(reinterpret_cast<char*>(&entry1),sizeof(entry1));
                    LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::INSERTKEY,this->page_id,key_val_tuple);
                    lsn_t cur_lsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(cur_lsn);
                    this->SetLSN(cur_lsn);
                    txn->GetRecordSet()->push_back(lg);
                }
                return true;
            }
            Value deleteData(int pos,bool& success,Transaction* txn){
                if (pos >= entry.size()) {
                    success=false;
                    return Value();
                }
                if (ENABLE_LOGGING) {
                    Tuple key_val_tuple;
                    key_val_tuple.DeserializeFrom(reinterpret_cast<char*>(&entry[pos]),
                                                  sizeof(entry[pos]));
                    LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                 LogRecordType::DELETEKEY,this->page_id,key_val_tuple);
                    lsn_t cur_lsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(cur_lsn);
                    this->setLSN(cur_lsn);
                    txn->GetRecordSet()->push_back(lg);
                }
                Value returnValue = entry.erase(pos).second;
                success=true;
                return returnValue;
            }


            /**
             *
             * @param key the key to delete
             * @param success
             * @return the value corresponding to the key
             */
            Value deleteData (const Key &key , bool &success , Transaction *txn) {
                auto posToErase = findData(key,success).second;


                if (!success) {
                    return Value();
                }
                Value returnValue=deleteData(posToErase,success,txn);
                if (entry.size() >= (maxEntryNum + 1) / 2 || this->parent == -1) {
                    return returnValue;
                }
                Page* parentpage= recordbpm.FetchPage
                        (this->parent , 0);
                InnerNode *parentnode = reinterpret_cast<InnerNode*>(parentpage->GetData());
                parentnode->baseTree=this->baseTree;
                LeafNode *rightcousin = nullptr , *leftcousin = nullptr;
                Page* leftcousinPage,*rightcousinPage;
                auto posInParent = parentnode->findSon(entry[0].first).second;
                //lend from cousin
                lsn_t prevlsn=txn->GetPrevLSN();
                if (posInParent != parentnode->sons.size() - 1) {
                     rightcousinPage= recordbpm.FetchPage(parentnode->sons[posInParent + 1] , 0);
                    rightcousin = reinterpret_cast<LeafNode*>(rightcousinPage->GetData());
                    rightcousin->baseTree=this->baseTree;
                }
                if (posInParent != 0) {
                     leftcousinPage= recordbpm.FetchPage(parentnode->sons[posInParent - 1] , 0);
                    leftcousin = reinterpret_cast<LeafNode*>(leftcousinPage->GetData());
                    leftcousin->baseTree=this->baseTree;
                }
                if (rightcousin && rightcousin->entry.size() > (maxEntryNum) / 2) {
                    auto tmp=rightcousin->entry.front();
                    rightcousin->deleteData(0,success,txn);
                    this->insertData(tmp,txn);
                    if (ENABLE_LOGGING) {
                        Tuple old_key_tuple,new_key_tuple;
                        old_key_tuple.DeserializeFrom(reinterpret_cast<const char*>
                        (&parentnode->keywords[posInParent]),sizeof(Key));
                        new_key_tuple.DeserializeFrom(reinterpret_cast<const char*>(&rightcousin->entry[0].first)
                        ,sizeof(Key));
                        LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                     LogRecordType::UPDATEKEY,parentnode->page_id,
                                     old_key_tuple,new_key_tuple);
                        lsn_t curlsn=logManager.AppendLogRecord(lg);
                        txn->SetPrevLSN(curlsn);
                        this->setLSN(curlsn);
                        txn->GetRecordSet()->push_back(lg);
                    }
                    parentnode->keywords[posInParent] = rightcousin->entry[0].first;
                    recordbpm.UnpinPage(rightcousin->page_id , true , 0);
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                    if(leftcousin)
                        recordbpm.UnpinPage(leftcousin->page_id , false , 0);
                } else if (leftcousin && leftcousin->entry.size() > (maxEntryNum) / 2) {
                    auto tmp=leftcousin->entry.back();
                    leftcousin->deleteData(leftcousin->entry.size()-1,success,txn);
                    this->insertData(tmp,txn);
                    if (ENABLE_LOGGING) {
                        Tuple old_key_tuple,new_key_tuple;
                        old_key_tuple.DeserializeFrom(reinterpret_cast<const char*>
                                                      (&parentnode->keywords[posInParent-1]),sizeof(Key));
                        new_key_tuple.DeserializeFrom(reinterpret_cast<const char*>(&this->entry[0].first)
                                ,sizeof(Key));
                        LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                     LogRecordType::UPDATEKEY,parentnode->page_id,
                                     old_key_tuple,new_key_tuple);
                        lsn_t curlsn=logManager.AppendLogRecord(lg);
                        txn->SetPrevLSN(curlsn);
                        this->setLSN(curlsn);
                        txn->GetRecordSet()->push_back(lg);
                    }
                    parentnode->keywords[posInParent - 1] = this->entry[0].first;
                    recordbpm.UnpinPage(leftcousin->page_id , true , 0);
                    recordbpm.UnpinPage(parentnode->page_id , true , 0);
                    if(rightcousin)
                        recordbpm.UnpinPage(rightcousin->page_id , false , 0);
                } else {
                    if (rightcousin) {
                        if (ENABLE_LOGGING) {
                            LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                    LogRecordType::MOVE,rightcousin->page_id,0,
                                    RID(this->page_id,this->entry.size()),rightcousin->entry.size());
                            lsn_t curlsn=logManager.AppendLogRecord(lg);
                            txn->SetPrevLSN(curlsn);
                            LogRecord lg2(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::UPDATEPTR,
                                    this->page_id,rightcousin->right);
                            curlsn=logManager.AppendLogRecord(lg2);
                            txn->SetPrevLSN(curlsn);
                            this->setLSN(curlsn);
                            txn->GetRecordSet()->push_back(lg);
                            txn->GetRecordSet()->push_back(lg2);
                        }
                        this->entry.merge(rightcousin->entry);
                        this->right = rightcousin->right;
                        recordbpm.UnpinPage(rightcousin->page_id , true , 0);
                        recordbpm.DeletePage(rightcousin->page_id , 0);
                        if(leftcousin)
                            recordbpm.UnpinPage(leftcousin->page_id , false , 0);
                        parentnode->deleteSon(posInParent + 1 , txn);
                        recordbpm.UnpinPage(parentnode->page_id , true , 0);
                    } else if (leftcousin) {
                        if (ENABLE_LOGGING) {
                            LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),
                                         LogRecordType::MOVE,this->page_id,0,
                                         RID(leftcousin->page_id,leftcousin->entry.size()),
                                         this->entry.size());
                            lsn_t curlsn=logManager.AppendLogRecord(lg);
                            txn->SetPrevLSN(curlsn);
                            LogRecord lg2(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::UPDATEPTR,
                                          leftcousin->page_id,this->right);
                            curlsn=logManager.AppendLogRecord(lg2);
                            txn->SetPrevLSN(curlsn);
                            this->setLSN(curlsn);
                            txn->GetRecordSet()->push_back(lg);
                            txn->GetRecordSet()->push_back(lg2);
                        }
                        leftcousin->entry.merge(this->entry);
                        leftcousin->right = this->right;
                        recordbpm.UnpinPage(leftcousin->page_id , true , 0);
                        parentnode->deleteSon(posInParent , txn);
                        recordbpm.UnpinPage(parentnode->page_id , true , 0);
                        recordbpm.UnpinPage(this->page_id , false , 0);
                        recordbpm.DeletePage(this->page_id , 0);
                    }

                }
                if (ENABLE_LOGGING) {
                    LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::CLR,prevlsn);
                    lsn_t curlsn=logManager.AppendLogRecord(lg);
                    txn->SetPrevLSN(curlsn);
                    txn->GetRecordSet()->push_back(lg);
                }
                return returnValue;
            }


        };

        /**
         *
         * @param address the address of the node that you want to read
         * @return a pointer to the node you want to read
         *
         * NOTE:you have to delete the node in the function that calls readNode
         */
//        Node *readNode (page_id_t address) {
//            auto page=recordbpm.FetchPage(address);
//            return reinterpret_cast<Node*>(page->GetData());
//        }

        /**
         * @throws NotEnoughMemoryException when there's too much thing in the file
         * @param isleaf whether the new node is a leaf node
         * @param left the left leaf node address to this new node
         * @param right the right leaf node address to this new node
         * @param parent the parent leaf node address to this new node
         * @return a pointer to the newnode
         *
         * NOTE:you have to delete the node in the function that calls newNode
         */
//        sjtu::Page *newNode (bool isleaf , page_id_t left , page_id_t right , page_id_t parent) {
//            page_id_t pageid;
//           auto page= recordbpm.NewPage(pageid);
//            Node* data=page->GetData();
//            new(data) Node(isleaf,parent,pageid,this,left,right);
//        }

        /**
         *
         * @param newnode the pointer to the node you want to update in memory
         *
         * NOTE: you can only update the node when it's been changed
         * in exactly the function where updateNode is called.And be aware to call
         * updateNode before any method call that may be involved with the node to update.
         */
//        void updateNode (Node *newnode,bool is_dirty) {
//            recordbpm.UnpinPage(newnode->page_id,is_dirty);
//        }

        /**
         *
         * @param nodeToDelete the pointer to the node that you want to delete
         *
         * NOTE: you need to delete the pointer after you call deleteNode
         */
//        void deleteNode (Node *nodeToDelete) {
//            recordbpm.DeletePage(nodeToDelete->page_id);
//        }

        page_id_t root;
        string<32> indexName;
        BulkLoading<Key,Value,Compare>* bulkLoading;


    /**
     * Update/Insert root page id in header page(where page_id = 0, header_page is
     * defined under include/page/header_page.h)
     * Call this method every time root page id is changed.
     * @param: insert_record      default value is false. When set to true,
     * insert a record <index_name, root_page_id> into header page instead of
     * updating it.
     */
    void updateRootPageId (int insert_record , Transaction *txn) {
            HeaderPage *header_page = static_cast<HeaderPage *>(
                    recordbpm.FetchPage(HEADER_PAGE_ID , 0));
            if (insert_record) {
                header_page->InsertRecord(indexName,root);
            } else {
                header_page->UpdateRecord(indexName,root);
            }
            if (ENABLE_LOGGING) {
                LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::UPDATEROOT,indexName,root);
                lsn_t curlsn=logManager.AppendLogRecord(lg);
                txn->SetPrevLSN(curlsn);
                txn->GetRecordSet()->push_back(lg);
            }
        recordbpm.UnpinPage(HEADER_PAGE_ID , true , 0);
        }
        /* helper method*/
        void insert (const pair <Key , Value> &entry , page_id_t h , Transaction *txn , page_id_t parentId = -1) {
            Page* thisPage= recordbpm.FetchPage(h , 0);
            Node *thisnode = reinterpret_cast<Node*>(thisPage->GetData());
            thisnode->baseTree=this;
            thisnode->parent=parentId;
            if (!thisnode->isLeaf) {
                auto tmp = ((InnerNode *) thisnode)->findSon(entry.first);
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
                insert(entry , tmp.first , txn , thisnode->page_id);
            } else {
                if (((LeafNode *) thisnode)->entry.size() >= maxEntryNum) {
                    lsn_t prevlsn=txn->GetPrevLSN();
                    ((LeafNode *) thisnode)->split(maxEntryNum / 2 , txn);
                    if (ENABLE_LOGGING) {
                        LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::CLR,prevlsn);
                        lsn_t curlsn=logManager.AppendLogRecord(lg);
                        txn->SetPrevLSN(curlsn);
                        txn->GetRecordSet()->push_back(lg);
                    }
                    LeafNode* rightnode= reinterpret_cast<LeafNode*> (recordbpm.FetchPage(thisnode->right , 0)->GetData());
                    if (Compare()( entry.first,rightnode->entry[0].first )) {
                        ((LeafNode *) thisnode)->insertData(entry , txn);
                    } else {
                        rightnode->insertData(entry,txn);
                    }
                    recordbpm.UnpinPage(rightnode->page_id , true , 0);
                } else {
                    ((LeafNode *) thisnode)->insertData(entry , txn);
                }
                recordbpm.UnpinPage(thisnode->page_id , true , 0);
            }

        }
        bool add (const pair <Key , Value> &entry , page_id_t h , Transaction *txn , page_id_t parentId = -1) {
            Page* thisPage= recordbpm.FetchPage(h , 0);
            Node *thisnode = reinterpret_cast<Node*>(thisPage->GetData());
            thisnode->baseTree=this;
            thisnode->parent=parentId;
            if (!thisnode->isLeaf) {
                auto tmp = ((InnerNode *) thisnode)->findSon(entry.first);
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
                return add(entry , tmp.first , txn , thisnode->page_id);
            } else {
                bool success;
                if (((LeafNode *) thisnode)->entry.size() >= maxEntryNum) {
                    lsn_t prevlsn=txn->GetPrevLSN();
                    ((LeafNode *) thisnode)->split(maxEntryNum / 2 , txn);
                    if (ENABLE_LOGGING) {
                        LogRecord lg(txn->GetTransactionId(),txn->GetPrevLSN(),LogRecordType::CLR,prevlsn);
                        lsn_t curlsn=logManager.AppendLogRecord(lg);
                        txn->SetPrevLSN(curlsn);
                        txn->GetRecordSet()->push_back(lg);
                    }
                    LeafNode* rightnode= reinterpret_cast<LeafNode*> (recordbpm.FetchPage(thisnode->right , 0)->GetData());
                    if (Compare()( entry.first,rightnode->entry[0].first )) {
                        success=((LeafNode *) thisnode)->addData(entry , txn);
                    } else {
                        success=rightnode->addData(entry,txn);
                    }
                    recordbpm.UnpinPage(rightnode->page_id , true , 0);
                } else {
                    success=((LeafNode *) thisnode)->addData(entry , txn);
                }
                recordbpm.UnpinPage(thisnode->page_id , true , 0);
                return success;
            }


        }


        /**
         *
         * @param key the key you want to search
         * @param h root node address
         * @param success
         * @return the value to the key
         *
         */
        Value find (const Key &key , page_id_t h,bool& success) {
            if (h == -1) {
                success=false;
                return Value();
            }
            Page* thisPage= recordbpm.FetchPage(h , 0);
            Node *thisnode = reinterpret_cast<Node*>(thisPage->GetData());
            thisnode->baseTree=this;
            Value result;
            if (!thisnode->isLeaf) {
                page_id_t tmp = (((InnerNode *) thisnode)->findSon(key)).first;
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
                result = find(key , tmp,success);
            } else {
                result = ((LeafNode *) thisnode)->findData(key,success).first;
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
            }
            return result;
        }

        /**
         *
         * @param key
         * @param h where to remove
         * @param success
         * @return the value you remove
         */
        Value remove (const Key &key , page_id_t h , bool &success , Transaction *txn , page_id_t parentId = -1) {
            if (h == -1) {
                success= false;
                return Value();
            }
            Page* thisPage= recordbpm.FetchPage(h , 0);
            Node *thisnode = reinterpret_cast<Node*>(thisPage->GetData());
            thisnode->baseTree=this;
            thisnode->parent=parentId;
            Value result;
            if (!thisnode->isLeaf) {
                auto tmp=((InnerNode *) thisnode)->findSon(key).first;
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
                result = remove(key , tmp , success , txn, thisnode->page_id);
            } else {

                result = ((LeafNode *) thisnode)->deleteData(key , success , txn);
                recordbpm.UnpinPage(thisnode->page_id , true , 0);
            }
            return result;
        }

        /*for test*/
        void completePrint (page_id_t h) {
            if (h == -1) {
                return;
            }

            std::queue<page_id_t> que;
            que.push(h);
            while (!que.empty()) {
                auto tmp = que.front();
                que.pop();
                Page* thisPage= recordbpm.FetchPage(tmp , 0);
                Node *thisnode = reinterpret_cast<Node*>(thisPage->GetData());
                thisnode->baseTree=this;
                if (thisnode->isLeaf) {
                    std::cout << "(leaf) address:" << thisnode->page_id << " entry:";
                    for (auto i = ((LeafNode *) thisnode)->entry.begin();
                         i != ((LeafNode *) thisnode)->entry.end(); i++) {
                        std::cout << (*i).second << " ";
                    }
                    std::cout << std::endl;
                } else {
                    std::cout << "(inner) "<<thisnode->page_id<<" keys:";
                    for (auto i = ((InnerNode *) thisnode)->keywords.begin();
                         i != ((InnerNode *) thisnode)->keywords.end(); i++) {
                        std::cout << (*i) << " ";
                    }
                    std::cout<<"sons: ";
                    for (auto i = ((InnerNode *) thisnode)->sons.begin();
                         i != ((InnerNode *) thisnode)->sons.end(); i++) {
                        std::cout<< (*i)<<" ";
                    }
                    std::cout << std::endl;
                    for (auto i = ((InnerNode *) thisnode)->sons.begin();
                         i != ((InnerNode *) thisnode)->sons.end(); i++) {
                        que.push(*i);
                    }
                }
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
            }
        }

        /**
         * save the bptree file and memorycontroller file
         */

    public:
        /**
         *
         *
         *
         *
         */
        Bptree (const string<32> &name) :
        indexName(name),root(INVALID_PAGE_ID),bulkLoading(nullptr){
            auto head= reinterpret_cast<HeaderPage*>(recordbpm.FetchPage(HEADER_PAGE_ID , 0));
            if(!head->GetRootId(name,root)) {
                root=INVALID_PAGE_ID;
                bulkLoading=new BulkLoading<Key,Value,Compare>(name);
            }
            recordbpm.UnpinPage(HEADER_PAGE_ID , false , 0);

        }

        ~Bptree () {
            if(bulkLoading)
            delete bulkLoading;
        }

        /**
         * if the key is already in the tree,change its value.Otherwise,put a new entry.
         * @param entry
         */
        void put (const pair <Key , Value> &entry , Transaction *txn) {

            if (root==INVALID_PAGE_ID) {
                bulkLoading->append(entry.first,entry.second);
                return;
            }
            insert(entry , root , txn , -1);

        }

        /**
        * if the key is already in the tree,change its value.Otherwise,put a new entry.
        * @param key
        * @param value
        */
        void put (const Key &key , const Value &value , Transaction *txn) {
            put(pair<Key , Value>(key , value) , txn);
        }
        /**
         * if the key is not in the tree,add ane entry,
         * otherwise, return false and do nothing
         * @param key
         * @param value
         * @return
         *
         */
        bool add (const Key &key , const Value &value , Transaction *txn) {
            if (root==INVALID_PAGE_ID ) {
                buildFromBottom(txn);
            }
            return add(pair<Key , Value>(key , value) , root , txn, -1);
        }
        //just for test
//        void simplePrint () {
//            LeafNode *headnode = (LeafNode *) readNode(head);
//            page_id_t p = headnode->right;
//            delete headnode;
//            while (p != tail) {
//                LeafNode *pnode = (LeafNode *) readNode(p);
//                for (auto i = pnode->entry.begin(); i != pnode->entry.end(); i++) {
//                    std::cout << (*i).first << " " << (*i).second << " ";
//                }
//                std::cout << "\t";
//                p = pnode->right;
//                delete pnode;
//            }
//            if (root == -1) {
//                return;
//            }
//            Node *tmp = readNode(root);
//            if (!tmp->isLeaf) {
//                std::cout << "root keyword:";
//                for (auto i = ((InnerNode *) tmp)->keywords.begin(); i != ((InnerNode *) tmp)->keywords.end(); i++) {
//                    std::cout << (*i) << " ";
//                }
//            }
//            std::cout << std::endl;
//            delete tmp;
//        }

        // just for test
        void completePrint () {
            completePrint(root);
        }

        /**
         * @throws NoSuchElementException when the key isn't in the tree
         * @param key
         * @return the value corresponding to the key
         */
        Value get (const Key &key , bool &success) {
            if (root==INVALID_PAGE_ID ) {
                Transaction* txn=transactionManager.Begin();
                buildFromBottom(txn);
                transactionManager.Commit(txn);
                delete txn;
            }
            return find(key , root,success);
        }

        /**
         *
         * @return how many entries there are in the tree
         */


        /**
         *
         * @param key
         * @return whether the key is in the tree
         */
        bool containsKey (const Key &key) {
            if (root==INVALID_PAGE_ID ) {
                Transaction* txn=transactionManager.Begin();
                buildFromBottom(txn);
                transactionManager.Commit(txn);
                delete txn;
            }
            bool success;
            find(key,root,success);
            return success;
        }

        /**
         * clear the files(irreversible)
         */
//        void clear () {
////
////            recordbpm.clear();
////            root = INVALID_PAGE_ID;
////            updateRootPageId(false);
////        }

        /**
         * @throws NoSuchElementException
         * @param key
         * @return the value corresponding to the key
         */
        Value remove (const Key &key , Transaction *txn) {
            if (root==INVALID_PAGE_ID ) {
                Transaction* txn=transactionManager.Begin();
                buildFromBottom(txn);
                transactionManager.Commit(txn);
                delete txn;
            }
            bool success;
            auto tmp= remove(key , root , success , txn , -1);
            if (success) {
                return tmp;
            } else {
                throw NoSuchElementException();
            }
        }

    private:
        /**
         *
         * @param key
         * @param h
         * @return the node where key is and the index of the key int the node
         */
        pair<Node*,int> findnode(Key key,page_id_t h,bool& success){
            if (h == -1) {
                success= false;
                return pair<Node*,int>(nullptr,-1);
            }
            Page* thisPage= recordbpm.FetchPage(h , 0);
            Node *thisnode = reinterpret_cast<Node*>(thisPage->GetData());
            thisnode->baseTree=this;
            pair<Node*,int> result;
            if (!thisnode->isLeaf) {
                page_id_t tmp = (((InnerNode *) thisnode)->findSon(key)).first;
                recordbpm.UnpinPage(thisnode->page_id , false , 0);
                result = findnode(key,tmp,success);
            } else {
                auto tmp= pair<Node*,int>(thisnode, ((LeafNode *) thisnode)->findData(key,success).second);
//                recordbpm.UnpinPage(thisnode->page_id, false);
                success=true;
                return tmp;
            }
            return result;
        }

    public:
        /**
         * find all the value whose keys are between start and end
         * @param start
         * @param end
         * @throw container_is_empty
         * @return a vector of the values [start,end)
         *
         *
         */
        void rangeSearch (Key start , Key end , vector <pair<Key , Value>> &result) {
            if (root==INVALID_PAGE_ID ) {
                Transaction* txn=transactionManager.Begin();
                buildFromBottom(txn);
                transactionManager.Commit(txn);
                delete txn;
            }
            bool success;
            auto startpos=findnode(start,root,success);
            if (!success) {
                throw container_is_empty();
            }
            char buf[PAGE_SIZE];
            memcpy(buf,startpos.first,PAGE_SIZE);
            recordbpm.UnpinPage(startpos.first->page_id , false , 0);
            startpos.first= reinterpret_cast<Node*>(buf);
            while(true){
                int i;
                for (i = startpos.second;i < ((LeafNode *) startpos.first)->entry.size() && (Compare()(((LeafNode *)startpos.first)->entry[i].first,end)); i++) {
                    result.push_back(((LeafNode *) startpos.first)->entry[i]);
                }
                if (i < ((LeafNode *) startpos.first)->entry.size()) {
                    break;
                }
                if (startpos.first->right == INVALID_PAGE_ID) {
                    break;
                }
//                diskManager->ReadPage(startpos.first->right, reinterpret_cast<char*>(startpos.first));
                recordbpm.UnpinPage(startpos.first->page_id , false , 0);
                Page* nextpage= recordbpm.FetchPage(startpos.first->right , 0);
                startpos.first= reinterpret_cast<Node*>(nextpage->GetData());
                startpos.second=0;
            }

        }
        /**
         *
         * @param key [start,end)中的start
         * @param answer
         * @param comparator 只有[start,end)满足的性质 比如key1.a==key.a
         *
         */
        void rangeSearch(Key key,vector<pair<Key,Value>> & answer,Comparator<Key>& comparator){
            if (root==INVALID_PAGE_ID ) {
                Transaction* txn=transactionManager.Begin();
                buildFromBottom(txn);
                transactionManager.Commit(txn);
                delete txn;
            }
            bool success;
            auto startpos=findnode(key,root,success);
            if (!success) {
                throw container_is_empty();
            }
            while(true){
                int i;
                for (i = startpos.second;i < ((LeafNode *) startpos.first)->entry.size() && (comparator(((LeafNode *)
                startpos.first)->entry[i].first,key)); i++) {
                    answer.push_back(((LeafNode *) startpos.first)->entry[i]);
                }
                if (i < ((LeafNode *) startpos.first)->entry.size()) {
                    recordbpm.UnpinPage(startpos.first->page_id , false , 0);
                    break;
                }

                if (startpos.first->right == INVALID_PAGE_ID) {
                    recordbpm.UnpinPage(startpos.first->page_id , false , 0);
                    break;
                }
                Page* nextpage= recordbpm.FetchPage(startpos.first->right , 0);
                recordbpm.UnpinPage(startpos.first->page_id , false , 0);
                startpos.first= reinterpret_cast<Node*>(nextpage->GetData());
                startpos.second=0;
            }
        }
        void buildFromBottom (Transaction *txn) {

            bulkLoading->sort();
            bulkLoading->ready_to_get_next();
            Page* rootPage;
            page_id_t rootpageid;
            rootPage= recordbpm.NewPage(rootpageid , 0);
            auto * rootnode= reinterpret_cast<LeafNode*>(rootPage->GetData());
            rootnode->init(rootpageid,-1,-1,1,txn,this);
            root = rootnode->page_id;
            updateRootPageId(true , txn);
            pair<Key,Value> entry;
            recordbpm.UnpinPage(rootnode->page_id , true , 0);
            while (true) {
                if(!bulkLoading->get_next(entry)) {
                    put(entry,txn);
                    break;
                }
                put(entry,txn);
            }
            bulkLoading->clear();
        }
    };

}

#endif //TTRS_BACKEND_B_TREE_H
