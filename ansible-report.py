import os,json,sqlite3,time,csv

db_file = "/tmp/ansible.db"
report_suffix = "_report.csv"
interesting_modules = ["shell"]
ignore_modules = ['setup','set_fact']
interesting_data = {"shell": "stdout","set_fact": 'ansible_facts'}

db = sqlite3.connect(db_file)
cur = db.cursor()

def log(host,play,task,data):
        global db_file
        db = sqlite3.connect(db_file)
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS " + scrub_var(play) + "_log (time,region,host,task,result,result_obj)")
        if type(data) == dict:
                invocation = data.pop('invocation',None)
                module = invocation['module_name']
                now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                if '_ansible_verbose_override' in data:
                        data = 'redacted'
                elif module in ignore_modules:
                        pass
                elif module in interesting_data.keys():
                        try:
                                idata = data[interesting_data[module]]
                                cur.execute("INSERT INTO " + scrub_var(play) + "_log VALUES(?,?,?,?,?,?);",(now,os.getenv('CCS_ENVIRONMENT','UNKNOWN'),host,task,str(idata),data))
                                db.commit()
                        except Exception as e:
                                print "Could not write data: %s" % e
                else:
                        if 'changed' in data.keys():
                                cur.execute("INSERT INTO " + scrub_var(play) + "_log VALUES(?,?,?,?,?,?)",(now,os.getenv('CCS_ENVIRONMENT','UNKNOWN'),host,task,data['changed'],data))
                                db.commit()
                        else:
                                print "Available keys for " + module + ": {}".format(data.keys())


def scrub_var(var):
        return ''.join(chr for chr in var if chr.isalnum())

def write_csv(play):
        global cur
        csv_data = {}
        keys = []
        print "Gathering resultant data..."
        for row in cur.execute('SELECT time,host,task,result FROM ' + scrub_var(play) + '_log as l1 where time = (SELECT max(time) from ' + scrub_var(play) + '_log as l2 where l1.host == l2.host and l1.task == l2.task);'):
                host = str(row[1])
                task = str(row[2])
                result = str(row[3])
                if len(csv_data) == 0:
                        csv_data.update({host: {}})
                if host not in csv_data.keys():
                        csv_data[host] = {}
                csv_data[host].update({task: result})
                if task not in keys:
                        keys.append(task)
        with open("{}{}".format(play,report_suffix),'w+') as csv_file:
                print "Writing results to {}{}...".format(play,report_suffix)
                c = csv.DictWriter(csv_file,fieldnames=keys)
                c.writeheader()
                for host in csv_data:
                        c.writerow(csv_data[host])


class CallbackModule():
        """
        Create CSV from Play
        """

        current_play = ""
        current_task = ""

        def runner_on_failed(self,host,res,ignore_errors=False):
                log(host,self.current_play,self.current_task, res)

        def runner_on_ok(self,host,res):
                log(host,self.current_play,self.current_task,res)

        def runner_on_unreachable(self,host,res):
                log(host,self.current_play,self.current_task,res)

        def playbook_on_play_start(self, play):
                #print "Starting play {}".format(play)
                self.current_play = play

        def playbook_on_task_start(self,task,args):
                #print "Starting task {} with data {}".format(task,args)
                self.current_task = task

        def playbook_on_stats(self,stats):
                write_csv(self.current_play)
