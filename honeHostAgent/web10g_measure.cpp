#include "web10g_measure.h"

using namespace std;

WorkTaskQueue input_queue;
estats_agent* agent = NULL;
map<string, string> output;
pthread_mutex_t output_lock;
string tuple_delimiter = ":";
string data_delimiter = "#";

extern "C" map<string, string> measure(map<int, string> sockByCid, map<string, string> sockByTuple, vector<int> statsToM)
{
    sort(statsToM.begin(), statsToM.end());
    statsToM.erase(unique(statsToM.begin(), statsToM.end()), statsToM.end());

    pthread_mutex_init(&output_lock, NULL);

    int num_threads = NUM_WORKERS;

    estats_error* err = NULL;
    estats_connection* c_head;
    estats_connection* c_pos;

    Chk(estats_agent_attach(&agent, ESTATS_AGENT_TYPE_LOCAL, NULL));
    Chk(estats_agent_get_connection_head(&c_head, agent));
    ESTATS_CONNECTION_FOREACH(c_pos, c_head) 
    {
        int cid;
        string sockfd;
        bool match = false;
        
        /* If no permission to access stats, ignore and go to next connection */
        if ((err = estats_connection_read_access(c_pos, R_OK)) != NULL) 
        {
            estats_error_free(&err);
            continue;
        }

        Chk(estats_connection_get_cid(&cid, c_pos));
        
        if (sockByCid.find(cid) != sockByCid.end())
        {
            // cid in sockByCid
            sockfd = sockByCid.find(cid)->second;
            match = true;
        }
        else if (sockByTuple.size() > 0)
        {
            // cid not in sockByCid
            struct estats_connection_spec spec; 
            struct spec_ascii spec_asc;
            
            Chk(estats_connection_get_connection_spec(&spec, c_pos));
            Chk(estats_connection_spec_as_strings(&spec_asc, &spec));
            string tuple = string(spec_asc.src_addr) + tuple_delimiter + string(spec_asc.src_port) + tuple_delimiter + string(spec_asc.dst_addr) + tuple_delimiter + string(spec_asc.dst_port);
            if (sockByTuple.find(tuple) != sockByTuple.end())
            {
                // tuple in sockByTuple
                sockfd = sockByTuple.find(tuple)->second;
                match = true;
            }
        }

        #ifdef _MATCH_ALL_
        match = true;
        #endif

        //cout << "match: " << match << " cid: " << cid << " sockfd: " << sockfd << endl;
        
        if (match)
        {
            input_queue.enqueue(cid, sockfd, c_pos);
        }
    }

    pthread_t workers[NUM_WORKERS];
    num_threads = ((sockByCid.size() + sockByTuple.size()) < NUM_WORKERS)?
                      (sockByCid.size() + sockByTuple.size()) : NUM_WORKERS;
    for (int i = 0; i < num_threads; i++)
    {
        pthread_create(&(workers[i]), NULL, worker_run, (void*)(&statsToM));
    }

    //cout << "Checkpoint 1" << endl;

    for (int i = 0; i < num_threads; i++)
    {
        pthread_join(workers[i], NULL);
    }

    //cout << "Checkpoint 2" << endl;

    //cout << "output size: " << output.size() << endl;
    //for (map<int, string>::iterator it = output.begin(); it != output.end(); it++)
    //{
        //cout << "output sockfd: " << it->first << " data: " << it->second << endl;
    //}
    
    //cout << "Checkpoint 3" << endl;

Cleanup:
    estats_agent_detach(&agent);
    agent = NULL;
    
    //cout << "Checkpoint 4" << endl;
    
    if (err != NULL)
    {
        PRINT_AND_FREE(err);
        output.clear();
    }

    return output;
}

extern "C" void* worker_run(void* arg)
{
    vector<int> statsToM = * (vector<int>*)(arg);
    int count_stats;
    int count_vars;
    WorkTask item;
    
    //cout << "Checkpoint 5" << endl;

    while (1)
    {
        item = input_queue.dequeue();
        if (item.cid < 0)
            break;

        //cout << "Checkpoint 6" << endl;
        //cout << "dequeue: " << item.cid << " " << item.sockfd << endl;
        
        if (item.conn != NULL)
        {
            stringstream ss;
            ss << item.cid;
            string stats = ss.str();

            estats_error* err = NULL;
            estats_var* var_head = NULL;
            estats_var* var_pos = NULL;
            estats_snapshot* snap = NULL;

            if ((err = estats_connection_read_access(item.conn, R_OK)) != NULL)
            {
                estats_error_free(&err);
                continue;
            }

            Chk(estats_snapshot_alloc(&snap, item.conn));
            Chk(estats_take_snapshot(snap));

            count_vars = 0;
            count_stats = 0;

            Chk(estats_agent_get_var_head(&var_head, agent));
            ESTATS_VAR_FOREACH(var_pos, var_head) 
            {
                if (count_vars < statsToM[count_stats])
                {
                    count_vars++;
                    continue;
                }
                
                estats_value* value = NULL;
                char* text = NULL;
                Chk(estats_snapshot_read_value(&value, snap, var_pos));
                Chk(estats_value_as_string(&text, value));

                stats += data_delimiter + string(text);

                free(text);
                estats_value_free(&value);

                count_vars++;
                count_stats++;

                if (count_stats == (int) statsToM.size())
                {
                    break;
                }
            }
            
            //cout << "Checkpoint 7" << endl;

            pthread_mutex_lock(&output_lock);
            output[item.sockfd] = stats;
            pthread_mutex_unlock(&output_lock);

            estats_snapshot_free(&snap);
        }
        
        //cout << "Checkpoint 8" << endl;

    Cleanup:
        continue;
    }

    return NULL;
}
