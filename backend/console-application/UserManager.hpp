//
// Created by jinho on 3/10/2019.
//

#ifndef TTRS_BACKEND_USERMANAGER_H
#define TTRS_BACKEND_USERMANAGER_H

#include <fstream>
#include "user.h"

namespace sjtu {

    class UserManager {
        unsigned long long usernum=0;
        sjtu::BufferPoolManager* bpm;
    public:
        friend class Console;
        UserManager (BufferPoolManager *bpm ) : bpm(bpm){
            std::fstream tmp("usernum");
            if (tmp.good()) {
                tmp.read(reinterpret_cast<char*>(&usernum),sizeof(usernum));
            }
        }
        ~UserManager (){
//            iofile.seekp(0);
//            iofile.write(reinterpret_cast<char*>(&usernum),sizeof(usernum));
//            iofile.close();

            std::fstream tmp("usernum",std::ios::out|std::ios::binary);
            tmp.write(reinterpret_cast<char*>(&usernum),sizeof(usernum));
            tmp.close();

        }

        unsigned long long registerUser(const string<40>& username,const string<20>& password,const string<20> &email,
                                        const string<20>&
                                        phone){
            auto newuser= user(username , password , email , phone , usernum+startno);
            Page* newuserPage;
            if(newuser.getSlot_id()==0) {
                int tmp;
                newuserPage= bpm->NewPage(tmp , 1);
            }
            else{newuserPage= bpm->FetchPage(newuser.getpage_id() , 1);}
            memcpy(newuserPage->GetData()+newuser.getSlot_id()*sizeof(user),&newuser,sizeof(user));
            usernum++;
            bpm->UnpinPage(newuser.getpage_id() , true , 1);
            return newuser.getuserid();
        }
        bool login(unsigned long long userid,const string<20>& password){
            auto userToLogin=search(userid);
            if (!userToLogin||userToLogin->getpassword() != password) {
                if (userToLogin) {
                    bpm->UnpinPage(userToLogin->getpage_id(),false,1);
                }
                return false;
            }
            bpm->UnpinPage(userToLogin->getpage_id() , false , 1);
            return true;
        }
        //note to unpin after use
        user* search(unsigned long long userid){
            if (userid >= usernum + startno || userid<startno) {
                return nullptr;
            }
            Page* thisuserPage= bpm->FetchPage(1+(userid - startno) / (PAGE_SIZE / sizeof(user)) , 1);
            return reinterpret_cast<user*>(thisuserPage->GetData()+sizeof(user)*((userid-startno)%(PAGE_SIZE/sizeof
                    (user))));
        }

        bool modify (unsigned long long id , const string<40> &name , const string<20> &password , const string<20>
        &email ,const string<20>& phone){
            if ( id>=usernum+startno) {
                return false;
            }
            auto thisuser=search(id);
            thisuser->setusername(name);
            thisuser->setemail(email);
            thisuser->setpassword(password);
            thisuser->setphonenum(phone);
            bpm->UnpinPage(thisuser->getpage_id() , true , 1);
            return true;
        }
        bool changePriviledge(unsigned long long userid1, unsigned long long userid2,int priviledge){
            if ((priviledge!=1 &&priviledge!=2)) {
                return false;
            }
            auto user1=search(userid1);
            if(!user1||(user1->getpriviledge()==1)) {
                if(user1)
                    bpm->UnpinPage(user1->getpage_id() , false , 1);
                return false;
            }
            auto user2=search(userid2);
            if (!user2 || (user2->getpriviledge()==2)) {
                bpm->UnpinPage(user1->getpage_id() , false , 1);
                if(user2)
                    bpm->UnpinPage(user2->getpage_id() , false , 1);
                return user2&&priviledge==2;
            }
            user2->setpriviledge(priviledge);
            bpm->UnpinPage(user2->getpage_id() , true , 1);
            bpm->UnpinPage(user1->getpage_id() , false , 1);
            return true;
        }
        void clear(){
            usernum=0;
        }

    };

}
#endif //TTRS_BACKEND_USERMANAGER_H
//query_profile 2018
//query_profile 2019
//query_profile 2020
//query_profile 2021
//query_profile 2022
//query_profile 2023
//query_profile 2024
//query_profile 2025
//query_profile 2026
//query_profile 2027
//query_profile 2028