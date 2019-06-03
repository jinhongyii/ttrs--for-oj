//
// Created by jinho on 3/12/2019.
//

//#include <bits/fcntl-linux.h>
#include "TicketManager.h"
bool sjtu::TicketManager::buyTicket (const sjtu::Userid &userid , int ticketnum , const sjtu::Trainid &trainid ,
                                     const sjtu::Loc &loc1 , const sjtu::Loc &loc2 , const int &date ,
                                     const sjtu::TicketKind &ticketKind) {
    auto txn=transactionManager.Begin();
    Tuple train_tuple,buy_tuple;
    RID train_rid,buy_rid;

    bool success= trainManager->queryTrain(trainid,train_tuple,train_rid,txn);
    train& tmp=*(reinterpret_cast<train*>(train_tuple.GetData()));
//    train_tuple.SerializeTo(reinterpret_cast<char*>(&tmp));
    if (!success || tmp.ticketleftPos1.GetPageId()==INVALID_PAGE_ID) {
        transactionManager.Abort(txn);
        delete txn;
        return false;
    }
    auto t = tikey{userid , date , tmp.catalog , trainid , loc1 , loc2};
    tival a{};
    bool keyAlreadyExists=false;
    buy_rid=buyticket.get(t,keyAlreadyExists);
    if(keyAlreadyExists) {
        buyRecord->GetTuple(buy_rid , buy_tuple , txn);
        buy_tuple.SerializeTo(reinterpret_cast<char *>(&a));
    }
    int i;
    for (i = 0; i < tmp.ticketkindnum && tmp.ticketKind[i] != ticketKind; i++);
    if (i == tmp.ticketkindnum) {
        transactionManager.Abort(txn);
        delete txn;
        return false;
    }
    int j,k;
    Tuple ltp_tuple,tl_tuple;
    char *ltp_buf,*tl_buf;
    int date1 = date , date2 = date;
    trainManager->loctimepriceTable->GetTuple(tmp.loctimepricePos,ltp_tuple,txn);
//    ltp_tuple.SerializeTo(ltp_buf);
    ltp_buf=ltp_tuple.GetData();
    Loc* locs= reinterpret_cast<Loc*>(ltp_buf);
    Time* timearrive= reinterpret_cast<Time*>(ltp_buf+sizeof(Loc)*tmp.locnum);
    Time* timestart= reinterpret_cast<Time*>(ltp_buf+(sizeof(Loc)+sizeof(Time))*tmp.locnum);
    if(!keyAlreadyExists) {
        for (j = 0; j < tmp.locnum && locs[j] != loc1; j++) {
            if (j != 0 && timearrive[j].compareTo(timestart[j - 1]) < 0) {
                date1++;
                date2++;
            }
        }
        k = j;
        for (; j < tmp.locnum && locs[j] != loc2; j++) {
            if (j != 0 && timearrive[j].compareTo(timestart[j - 1]) < 0) {
                date2++;
            }
        }
    } else{
        for (j = 0; j < tmp.locnum && locs[j] != loc1; j++);
        k = j;
        for (; j < tmp.locnum && locs[j] != loc2; j++) ;
    }
    if (j == tmp.locnum) {
        transactionManager.Abort(txn);
        delete txn;
        return false;
    }
    if(date<datenum/3) {
        trainManager->ticketleftTable1->GetTuple(tmp.ticketleftPos1,tl_tuple,txn);
        tl_buf=tl_tuple.GetData();
        short* tickets= reinterpret_cast<short*>((date * tmp.ticketkindnum * (tmp.locnum - 1) + i * (tmp.locnum - 1) +k) * sizeof(short)+tl_buf);
        for (int p = 0; p < j - k; p++) {
            if (tickets[p] >= ticketnum) {
                tickets[p] -= ticketnum;
            } else {
                transactionManager.Abort(txn);
                delete txn;
                return false;
            }
        }
//        tl_tuple.DeserializeFrom(tl_buf,tl_tuple.GetLength());
        trainManager->ticketleftTable1->UpdateTuple(tl_tuple,tmp.ticketleftPos1,txn);
    } else if (date < 2 * datenum / 3) {
        trainManager->ticketleftTable2->GetTuple(tmp.ticketleftPos2,tl_tuple,txn);
        tl_buf=tl_tuple.GetData();
        short* tickets= reinterpret_cast<short*>(((date-datenum/3) * tmp.ticketkindnum * (tmp.locnum - 1) + i * (tmp.locnum - 1) +k) * sizeof(short)+tl_buf);
        for (int p = 0; p < j - k; p++) {
            if (tickets[p] >= ticketnum) {
                tickets[p] -= ticketnum;
            } else {
                transactionManager.Abort(txn);
                delete txn;
                return false;
            }
        }
//        tl_tuple.DeserializeFrom(tl_buf,tl_tuple.GetLength());
        trainManager->ticketleftTable2->UpdateTuple(tl_tuple,tmp.ticketleftPos2,txn);
    } else{
        trainManager->ticketleftTable3->GetTuple(tmp.ticketleftPos3,tl_tuple,txn);
        tl_buf=tl_tuple.GetData();
        short* tickets= reinterpret_cast<short*>(((date-2*datenum/3) * tmp.ticketkindnum * (tmp.locnum - 1) + i * (tmp
                .locnum- 1) +k) * sizeof(short)+tl_buf);
        for (int p = 0; p < j - k; p++) {
            if (tickets[p] >= ticketnum) {
                tickets[p] -= ticketnum;
            } else {
                transactionManager.Abort(txn);
                delete txn;
                return false;
            }
        }
//        tl_tuple.DeserializeFrom(tl_buf,tl_tuple.GetLength());
        trainManager->ticketleftTable3->UpdateTuple(tl_tuple,tmp.ticketleftPos3,txn);
    }
    if(!keyAlreadyExists) {
        Time time1 = timestart[k];
        Time time2 = timearrive[j];

        TicketPrice prices[10];
        memcpy(prices,ltp_buf+sizeof(Loc)+3*sizeof(Time)*tmp.locnum+sizeof(TicketPrice)*tmp
                .ticketkindnum*k,sizeof(TicketPrice)*tmp.ticketkindnum);
        memcpy(prices+tmp.ticketkindnum,ltp_buf+(sizeof(Loc)+3*sizeof(Time))*tmp.locnum+sizeof(TicketPrice)*tmp.ticketkindnum*j,sizeof(TicketPrice)*tmp.ticketkindnum);
        a.startdate=date1;
        a.enddate=date2;
        a.starttime=time1;
        a.endtime=time2;
        for (int o = 0; o < tmp.ticketkindnum; o++) {
            a.ticketKind[o] = tmp.ticketKind[o];
        }
        for (int o = 0; o < tmp.ticketkindnum; o++) {
            a.ticketPrice[o] = prices[o + tmp.ticketkindnum] - prices[o];
        }
        a.numBought[i] += ticketnum;
        a.loc1order = k;
        a.loc2order = j;
    } else {
        a.numBought[i] += ticketnum;

    }
    buy_tuple.DeserializeFrom(reinterpret_cast<char*>(&a),sizeof(a));
    if(keyAlreadyExists) {
        buyRecord->UpdateTuple(buy_tuple , buy_rid , txn);
    } else {
        buyRecord->InsertTuple(buy_tuple,buy_rid,txn);
        buyticket.put(t , buy_rid , txn);
    }
    transactionManager.Commit(txn);
    delete txn;
    return true;
}

void sjtu::TicketManager::queryOrder (const sjtu::Userid &userid , const int &date , const sjtu::CatalogList &
catalogList,std::ostream& os) {
    struct ticketcmp:public Comparator<tikey>{
        bool operator()(const tikey& k1,const tikey& k2)const{
            return k1.userid==k2.userid && k1.date==k2.date && k1.catalog==k2.catalog;
        }
    };
    vector<pair<tikey , RID>> ticketrecords[10];
    int linenum = 0;
    for (int pos = 0; pos < catalogList.size(); pos++) {
        auto catalog = catalogList[pos];
        ticketcmp cmp;
        buyticket.rangeSearch(tikey{userid , date , catalog , Trainid() , Loc(),Loc()} ,ticketrecords[pos],cmp);
        linenum += ticketrecords[pos].size();
    }
    os << linenum << std::endl;
    Tuple buy_tuple;
    char buy_buf[300];
    tival* ti= reinterpret_cast<tival*>(buy_buf);
    for (int pos = 0; pos < catalogList.size(); pos++) {
        for (int i = 0; i < ticketrecords[pos].size(); i++) {
            auto ticketinfo = ticketrecords[pos][i];
            buyRecord->GetTuple(ticketinfo.second,buy_tuple, nullptr);
            buy_tuple.SerializeTo(buy_buf);
            os << ticketinfo.first.trainid << " " << ticketinfo.first.loc1 << " " << numtodate(ti->startdate)<< " " <<
            ti->starttime << " " << ticketinfo.first.loc2 << " "<< numtodate(ti->enddate) << " " <<
            ti->endtime << " ";
            for (int j = 0; j < 5 && !(ti->ticketKind[j] == string<20>("")); j++) {
                os << ti->ticketKind[j] << " " << ti->numBought[j] << " "
                          << ti->ticketPrice[j] << " ";
            }
            os << std::endl;
        }
    }
}

bool sjtu::TicketManager::refundTicket (const sjtu::Userid &userid , int refundnum , const sjtu::Trainid &trainid ,
                                        const sjtu::Loc &loc1 , const sjtu::Loc &loc2 , const int &date ,
                                        const sjtu::TicketKind &ticketKind) {
    auto txn=transactionManager.Begin();
    bool success=false;
    auto train = trainManager->queryTrain(trainid , success , txn);

    auto deleted_rid = buyticket.get(tikey{userid , date , train.catalog , trainid , loc1 , loc2} ,
                                           success);
    Tuple deleted_tuple,tl_tuple;
if(success) {
    buyRecord->GetTuple(deleted_rid,deleted_tuple,txn);
    tival ti;
    deleted_tuple.SerializeTo(reinterpret_cast<char*>(&ti));
    int i;
    for (i = 0; i < train.ticketkindnum && train.ticketKind[i] != ticketKind; i++);
    if (i == train.ticketkindnum) {
        transactionManager.Abort(txn);
        delete txn;
        return false;
    }
    if (ti.numBought[i] < refundnum) {
        transactionManager.Abort(txn);
        delete txn;
        return false;
    } else {
        ti.numBought[i] -= refundnum;
    }
    char tl_buf[6000];
    if(date<datenum/3) {
        trainManager->ticketleftTable1->GetTuple(train.ticketleftPos1,tl_tuple,txn);
        tl_tuple.SerializeTo(tl_buf);
        short*ticketleftnum= reinterpret_cast<short*>(tl_buf+date * sizeof(short) * train.ticketkindnum * (train.locnum - 1) +i * sizeof(short) * (train.locnum - 1) + sizeof(short) * ti
                                                      .loc1order);
        for (int j = 0; j < (ti.loc2order - ti.loc1order); j++) {
            ticketleftnum[i] += refundnum;
        }
        tl_tuple.DeserializeFrom(tl_buf,sizeof(tl_tuple));
        trainManager->ticketleftTable1->UpdateTuple(tl_tuple,train.ticketleftPos1,txn);
    } else if (date < 2 * datenum / 3) {
        trainManager->ticketleftTable2->GetTuple(train.ticketleftPos2,tl_tuple,txn);
        tl_tuple.SerializeTo(tl_buf);
        short*ticketleftnum= reinterpret_cast<short*>(tl_buf+date * sizeof(short) * train.ticketkindnum * (train.locnum - 1) +i * sizeof(short) * (train.locnum - 1) + sizeof(short) * ti
                .loc1order);
        for (int j = 0; j < (ti.loc2order - ti.loc1order); j++) {
            ticketleftnum[i] += refundnum;
        }
        tl_tuple.DeserializeFrom(tl_buf,sizeof(tl_tuple));
        trainManager->ticketleftTable2->UpdateTuple(tl_tuple,train.ticketleftPos2,txn);
    } else {
        trainManager->ticketleftTable3->GetTuple(train.ticketleftPos3,tl_tuple,txn);
        tl_tuple.SerializeTo(tl_buf);
        short*ticketleftnum= reinterpret_cast<short*>(tl_buf+date * sizeof(short) * train.ticketkindnum * (train.locnum - 1) +i * sizeof(short) * (train.locnum - 1) + sizeof(short) * ti
                .loc1order);
        for (int j = 0; j < (ti.loc2order - ti.loc1order); j++) {
            ticketleftnum[i] += refundnum;
        }
        tl_tuple.DeserializeFrom(tl_buf,sizeof(tl_tuple));
        trainManager->ticketleftTable3->UpdateTuple(tl_tuple,train.ticketleftPos3,txn);
    }
    deleted_tuple.DeserializeFrom(reinterpret_cast<char*>(&ti),sizeof(ti));
    buyRecord->UpdateTuple(deleted_tuple,deleted_rid,txn);
    transactionManager.Commit(txn);
    delete txn;
    return true;
} else {
    transactionManager.Abort(txn);
    delete txn;
    return false;
}



}


int sjtu::datetonum (const sjtu::Date &date) {
    return (date[8] - '0') * 10 + date[9] - '0' - 1+(date[6]-'6')*30;
}

sjtu::Date sjtu::numtodate (int date) {
    auto i = string<10>("2019-06-");
    date++;
    if (date > 30) {
        i[6]='7';
        date%=30;
    }
    i+=(date / 10 + '0');
    i += (date % 10 + '0');
    return i;
}
