#include<stdio.h>
#include<bits/stdc++.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<string.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/sendfile.h>
#include<fcntl.h>
#include<netdb.h>
#include<unistd.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
using namespace std;
using namespace rapidjson;

Document document;
unsigned long long reqID=0;

map<string,string> MyKeyValueMap;
map<string,string> SuccessorKeyValueMap;

void
error(const char *msg){
    perror(msg);
    exit(1);
}


string prepareREGISTERmessageinjson(string hostid,int port)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"registerslavereq\", \"slavehostid\" : \""+hostid+"\", \"slaveport\" : "+to_string(port)+" } ";
    reqID++;
    return str;
}


string preparePREPAREACKSLAVEmessageinjson(int id)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"prepareackpc\", \"prepared\" : "+to_string(id)+" } ";
    reqID++;
    return str;
}


string prepareCOMMITACKSLAVEmessageinjson(int id)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"commitackpc\", \"committed\" : "+to_string(id)+" } ";
    reqID++;
    return str;
}

string preparePUTmessageinjson(string key,string value)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"putreq\", \"key\" : \""+key+"\", \"value\" : \""+value+"\" } ";
    reqID++;
    return str;
}

string prepareGETACKmessageinjson(string key,string value)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"getack\", \"key\" : \""+key+"\", \"value\" : \""+value+"\" } ";
    reqID++;
    return str;
}


string prepareDELmessageinjson(string key)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"delreq\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}


string prepareBYEmessageinjson()
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"byereq\" } ";
    cout<<"Byee...!"<<endl;
    reqID++;
    return str;
}



string prepareGETmessageinjson(string key)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"getreq\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}

int main(int argc,char *argv[]){

    int sock;
    if(argc!=5)
        error("Not Enough Arguments!!");
    struct sockaddr_in server;
    struct hostent *serverhost;
    char *serverPort = argv[2];
    char *serverHost = argv[1];
    char *slavePort = argv[4];
    char *slaveIp = argv[3];
    sock=socket(AF_INET, SOCK_STREAM, 0);
    if(sock<0)
        error("Error Creating Socket");
    server.sin_family=AF_INET;
    server.sin_port=htons(atoi(serverPort));
    server.sin_addr.s_addr=0;
    serverhost=gethostbyname(serverHost);
    bcopy((char *)serverhost->h_addr,(char *)&server.sin_addr.s_addr,serverhost->h_length);
    if (connect(sock,(struct sockaddr*)&server,sizeof(server)) < 0)
        error("Error connect");
    //char buffer[200];
    //sprintf(buffer,"%s#%s#%s","0",clientIp,clientPort);
    string str(slaveIp);
    string buff=prepareREGISTERmessageinjson(str,atoi(slavePort));
    cout<<buff<<endl;
    send(sock,buff.c_str(),200,0);

    char ack[300];
    recv(sock, ack, 300, 0);
    string ackstring(ack);
    cout<<ackstring<<endl;




    if (document.ParseInsitu(ack).HasParseError())
    {
        cout<<"Error Parsing Document"<<endl;
        return 0;
        //pvalue=0;
    }
       // return;
    else{
    assert(document.IsObject());

    assert(document.HasMember("registered"));
    assert(document["registered"].IsInt());


    printf("\nParsing to document succeeded.\n");

    assert(document["port"].IsInt());

    //pvalue=1;

    cout<<"This is registerid:"<<document["registered"].GetInt()<<endl;
    if(document["registered"].GetInt()>=1)
         cout<<"Registration Successful Got userid:"<<document["registered"].GetInt()<<endl;
    else cout<<"Registration Unsuccessful"<<endl;

    //document["port"].GetInt()


    int sock3;
    struct sockaddr_in server;
    struct hostent *serverhost;
    //char *serverPort = argv[2];
    //char *serverHost = argv[1];
    //char *clientPort = argv[4];
    //char *clientIp = argv[3];
    sock3=socket(AF_INET, SOCK_STREAM, 0);
    cout<<"Port"<<document["port"].GetInt()<<endl;
    if(sock3<0)
        error("Error Creating Socket");
    server.sin_family=AF_INET;
    server.sin_port=htons(document["port"].GetInt());
    server.sin_addr.s_addr=0;
    serverhost=gethostbyname(serverHost);
    bcopy((char *)serverhost->h_addr,(char *)&server.sin_addr.s_addr,serverhost->h_length);
    if (connect(sock3,(struct sockaddr*)&server,sizeof(server)) < 0)
        error("Error connect");


    cout<<"I got port :"<<document["port"].GetInt()<<endl;


    while(1)
    {
        char masterCommand[300];
        recv(sock3,masterCommand,300,0);
        cout<<masterCommand<<endl;
        string masterCommandString(masterCommand);
        if (document.ParseInsitu(masterCommand).HasParseError())
        {
            cout<<"DocumentParsing error"<<endl;
        }
        else
        {
            cout<<"Parsing successful"<<endl;
            cout<<masterCommandString<<endl;
            if(strcmp(document["reqtype"].GetString(),"getreq")==0)
            {
                string responseValue(document["key"].GetString());

                cout<<MyKeyValueMap[responseValue]<<endl;
                string corval=MyKeyValueMap[responseValue];

                send(sock3,(prepareGETACKmessageinjson(responseValue,corval)).c_str(),300,0);

            }

            else if(strcmp(document["reqtype"].GetString(),"prepareput")==0)
            {

                cout<<document["reqtype"].GetString()<<endl;

                int successor=document["prepare"].GetInt();
                cout<<successor<<endl;
                string getmessageinjson1 = preparePREPAREACKSLAVEmessageinjson(1);
                            send(sock3,getmessageinjson1.c_str(),300,0);

                            char receiveResponseCommand[300];
                            recv(sock3,receiveResponseCommand,300,0);
                cout<<receiveResponseCommand<<endl;
                        string key(document["key"].GetString());
                        string corrvalue(document["value"].GetString());

                        if (document.ParseInsitu(receiveResponseCommand).HasParseError())
                        {
                            cout<<"Parse Error"<<endl;
                        }
                        else
                        {
                            if(document["commit"].GetInt()==1)
                            {
                                    if(successor==0)
                                            MyKeyValueMap[key]=corrvalue;
                                    else    SuccessorKeyValueMap[key]=corrvalue;
                            }
                            else
                            {


                            }
                        }
            }


            else if(strcmp(document["reqtype"].GetString(),"preparedel")==0)
            {

                //int successor=document["prepare"].GetInt();
                //cout<<document["reqtype"].GetString()<<endl;

                //int successor=document["prepare"].GetInt();
                //cout<<successor<<endl;
                string getmessageinjson1 = preparePREPAREACKSLAVEmessageinjson(1);
                send(sock3,getmessageinjson1.c_str(),300,0);

                char receiveResponseCommand[300];
                recv(sock3,receiveResponseCommand,300,0);
                cout<<receiveResponseCommand<<endl;
                string key(document["key"].GetString());
                //string corrvalue(document["value"].GetString());

                        if (document.ParseInsitu(receiveResponseCommand).HasParseError())
                        {
                            cout<<"Parse Error"<<endl;
                        }
                        else
                        {
                            if(document["commit"].GetInt()==1)
                            {
                                    if(MyKeyValueMap.find(key)!=MyKeyValueMap.end())
                                            MyKeyValueMap.erase(key);
                                    else    SuccessorKeyValueMap.erase(key);
                            }
                            else
                            {


                            }
                        }
                /*string getmessageinjson1 = preparePREPAREACKSLAVEmessageinjson(1);
                send(sock3,getmessageinjson1.c_str(),300,0);

                char receiveResponseCommand[300];
                recv(sock3,receiveResponseCommand,300,0);

                if(document["commit"].GetInt()==1)
                {
                        string key(document["key"].GetString());
                        string corrvalue(document["value"].GetString());
                        //if(successor==0)
                        if(MyKeyValueMap.find(key)!=MyKeyValueMap.end())
                            MyKeyValueMap.erase(key);
                        if(SuccessorKeyValueMap.find(key)!=SuccessorKeyValueMap.end())
                            SuccessorKeyValueMap.erase(key);

                }
                else
                {


                }*/
            }


        }

    }

    /*while(1)
    {
        cout<<"Select A Request for server:"<<endl;
        cout<<"1.GET REQUEST"<<endl;
        cout<<"2.PUT REQUEST"<<endl;
        cout<<"3.DEL REQUEST"<<endl;
        cout<<"4.End Connection"<<endl;
        int selection;
        cin>>selection;
        if(selection==1)
        {
            cout<<"Please enter a Key to find : ";
            string key;
            cin>>key;
            string getmessageinjson = prepareGETmessageinjson(key);
            cout<<getmessageinjson;
            //jsonstringtodocument(getmessageinjson);
            send(sock3,getmessageinjson.c_str(),100,0);
            cout<<"Sent Successfully"<<endl;
        }
        else if(selection==2)
        {
            cout<<"Please enter Key : ";
            string key;
            cin>>key;
            cout<<"Please enter Value : ";
            string value;
            cin>>value;

            string getmessageinjson = preparePUTmessageinjson(key,value);
            cout<<getmessageinjson;
            //jsonstringtodocument(getmessageinjson);
            send(sock3,getmessageinjson.c_str(),100,0);
            cout<<"Sent Successfully"<<endl;
        }
        else if(selection==3)
        {
            cout<<"Please enter Key to delete : ";
            string key;
            cin>>key;
            string getmessageinjson = prepareDELmessageinjson(key);
            cout<<getmessageinjson;
            //jsonstringtodocument(getmessageinjson);
            send(sock3,getmessageinjson.c_str(),100,0);
            cout<<"Sent Successfully"<<endl;
        }
        else
        {
            string getmessageinjson = prepareBYEmessageinjson();
            cout<<getmessageinjson;
            send(sock3,getmessageinjson.c_str(),100,0);
            close(sock3);
            break;
        }

    }*/

    }

    return 0;
}

