//
// Created by jinho on 3/12/2019.
//
#pragma once
#ifndef TTRS_BACKEND_TICKETMANAGER_H
#define TTRS_BACKEND_TICKETMANAGER_H


#include "../util.h"
#include "TrainManager.h"

namespace sjtu {
    int datetonum (const Date &date);

    Date numtodate (int date);

    struct tikey {
        Userid userid;
        int date;
        Catalog catalog;
        Trainid trainid;
        Loc loc1 , loc2;
    };
    struct tival {
        int numBought[5];
        int startdate = 0;
        int enddate = 0;
        Time starttime;
        Time endtime;
        TicketPrice ticketPrice[5];
        TicketKind ticketKind[5];
        char loc1order , loc2order;
    };

    struct ticompare :public Comparator<tikey>{
        bool operator() (const tikey &key1 , const tikey &key2) const {
            int cmp = key1.userid-key2.userid;
            if (cmp != 0) {
                return cmp < 0;
            } else {
                cmp = key1.date - key2.date;
                if (cmp != 0) {
                    return cmp < 0;
                } else {
                    cmp = key1.catalog - key2.catalog;
                    if (cmp != 0) {
                        return cmp < 0;
                    } else {
                        cmp = key1.trainid.compareTo(key2.trainid);
                        if (cmp != 0) {
                            return cmp < 0;
                        } else {
                            cmp = key1.loc1.compareTo(key2.loc1);
                            if (cmp != 0) {
                                return cmp < 0;
                            } else {
                                cmp = key1.loc2.compareTo(key2.loc2);
                                return cmp < 0;
                            }
                        }
                    }
                }
            }
        }
    };

    class TicketManager {
        Bptree<tikey , RID , ticompare> buyticket;
        TableHeap* buyRecord;
    public:
        TrainManager *trainManager;
    public:
        TicketManager () : buyticket("buy_ticketindex") {
            auto txn=transactionManager.Begin();
            auto head= reinterpret_cast<HeaderPage*>(recordbpm.FetchPage(HEADER_PAGE_ID , 0));
            page_id_t root_id;
            if(head->GetRootId("buy_ticket",root_id)) {
                buyRecord=new TableHeap(&recordbpm,&logManager,root_id);
            } else {
                buyRecord=new TableHeap(&recordbpm,&logManager,txn);
                head->InsertRecord("buy_ticket",buyRecord->GetFirstPageId());
            }
            recordbpm.UnpinPage(HEADER_PAGE_ID , true , 0);
            transactionManager.Commit(txn);
            delete txn;
        }
        ~TicketManager (){
            delete buyRecord;
        }
        bool
        buyTicket (const Userid &userid , int ticketnum , const Trainid &trainid , const Loc &loc1 , const Loc &loc2 ,
                   const int &date , const TicketKind &ticketKind);

        void queryOrder (const Userid &userid , const int &date , const CatalogList& catalogList,std::ostream& os);

        bool refundTicket (const Userid &userid , int refundnum , const Trainid &trainid , const Loc &loc1 ,
                           const Loc &loc2 ,
                           const int &date , const TicketKind &ticketKind);

    };
}


#endif //TTRS_BACKEND_TICKETMANAGER_H
