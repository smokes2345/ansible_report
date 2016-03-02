import os,json,csv,sqlite3,time

db_file = "/tmp/ansible.db"
interesting_modules = ["shell"]
interesting_data = {"shell": "stdout"}
header_map = {'shell': 'cmd'}

print "Opening database {}".format(db_file)
db = sqlite3.connect(db_file)
cur = db.cursor()

def log(host,play,task,data):
        global cur
        cur.execute("CREATE TABLE IF NOT EXISTS log_" + scrub_var(play) + " (time,host,task,result)")
        if type(data) == dict:
                invocation = data.pop('invocation',None)
                module = invocation['module_name']
                if '_ansible_verbose_override' in data:
                        data = 'redacted'
                elif module in interesting_modules:
                        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        try:
                                data = data[interesting_data[module]]
                                cur.execute("INSERT INTO log_" + scrub_var(play) +" VALUES(?,?,?,?);",(now,host,task,data))
                                db.commit()
                        except Exception as e:
                                print "Could not write data: %s" % e

def scrub_var(var):
	return ''.join(chr for chr in var if chr.isalnum())

def write_csv():
	global cur
	with open("results.csv") as csv_file:
		csv = csv.writer(csv_file)
		csv_row = []
		csv.writerow([i[0] for i in cur.description])
		for row in cur.execute('SELECT time,host,play,task,result FROM log as l1 where time = (SELECT max(time) from log as l2 where l1.host == l2.host and l1.play == l2.play and l1.task == l2.task);'):
			csv_row = []


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

#        def playbook_on_stats(self,stats):

