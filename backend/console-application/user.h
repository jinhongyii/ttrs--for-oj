//
// Created by jinho on 3/10/2019.
//

#ifndef TTRS_BACKEND_USER_H
#define TTRS_BACKEND_USER_H

#include "../util.h"
#include <iostream>
#include "buffer/buffer_pool_manager.h"
namespace sjtu {

    class user {
        friend class UserManager;
        //todo:not right,but will pass test
        string<20> password;
        string<20> email;
        string<12> username;
        string<11> phonenum;
        unsigned int userid=~0u;


        int priviledge;
    public:
        user()= default;
        user (const string<40> &username , const string<20> &password , const string<20> &email ,
              const string<20>& phonenum , unsigned long long userid)
                :username
                         (username),password(password),email(email),phonenum(phonenum),userid(userid)
        {
            if(userid==startno) {
                priviledge=2;
            } else {
                priviledge=1;
            }
        }

        user(const user& other)= default;

        user&operator=(const user& other){
            new(this)user(other);
            return *this;
        }

        inline void setpriviledge(int priviledge){
            if (priviledge != 1 && priviledge != 2 && priviledge!=0) {
                throw InvalidArgumentException();
            }
            this->priviledge = priviledge;
        }

        inline void setusername(const string<40>& name){username=name;}
        inline void setemail (const string<20> &email){this->email=email;}
        inline void setpassword(const string<20>& password){this->password=password;}
        inline void setphonenum(const string<20>& phone){phonenum=phone;}
        inline string<12>& getusername (){return username;}
        inline string<20>& getpassword(){return password;}
        inline unsigned  getuserid(){return userid;}
        inline int getpriviledge(){return priviledge;}
        inline string<20>& getemail(){return email;}
        inline string<11> getphone(){return phonenum;}
        friend class userComparator;
        inline unsigned getpage_id (){ return 1+(userid-startno)/maxuserPerPage;}
        inline unsigned getSlot_id(){ return (userid-startno)%maxuserPerPage;}
        friend inline std::ostream&operator<<(std::ostream& os,const user& user){
            os<<user.username<<" "<<user.email<<" "<<user.phonenum<<" "<<user.priviledge;
            return os;
        }
    };
    class userComparator{
        bool operator()(user a,user b){
            return a.userid<b.userid;
        }
    };



}


#endif //TTRS_BACKEND_USER_H
