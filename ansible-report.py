import os,json,csv,sqlite3,time

db_file = "/tmp/ansible.db"
interesting_modules = ["shell"]
interesting_data = {"shell": "stdout"}
header_map = {'shell': 'cmd'}

print "Opening database {}".format(db_file)
db = sqlite3.connect(db_file)
cur = db.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS log_? (time,host,play,task,result)",)

def log(host,play,task,data):
        global cur
        if type(data) == dict:
                invocation = data.pop('invocation',None)
                module = invocation['module_name']
                if '_ansible_verbose_override' in data:
                        data = 'redacted'
                elif module in interesting_modules:
                        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        try:
                                data = data[interesting_data[module]]
                                cur.execute("INSERT INTO log VALUES(?,?,?,?,?);",(now,host,play,task,data))
                                db.commit()
                        except Exception as e:
                                print "Could not write data: %s" % e


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

