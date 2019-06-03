//
// Created by jinho on 3/11/2019.
//

#include "Console.h"
using namespace sjtu;
int main(){
    std::ios::sync_with_stdio(false);
    std::cin.tie(0);
    std::cout.tie(0);
    std::fstream f("../backend/test_kit/8/6.in");
    Console console;
    while (true) {
        if(console.processline(std::cin,std::cout)) {
            break;
        }
//        auto page=recordbpm.FetchPage(118);
//        std::cerr<<(page?page->GetPageId():-1)<<std::endl;
//        recordbpm.UnpinPage(118,false);
    }


}