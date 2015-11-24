#!/usr/bin/env python

import cmd
import sys
import threading
import pkgutil

import kernel.unisrt
import apps

class NREShell(cmd.Cmd):
    
    def __init__(self):
        # before I could ever implement the runtime environment as a separate
        # stand alone daemon, it'd be an object of the shell
        self.unisrt = kernel.unisrt.UNISrt()        
        
        self.prompt = '> '
        cmd.Cmd.__init__(self)
        
    def do_lm(self, model):
        '''
        Usage: lm <model name>
        list instances of requested model
        '''
        if model in self.unisrt._resources:
            print "model {m} has following instances:".format(m = model)
            model_objects = getattr(self.unisrt, model)
            for instance in model_objects['existing']:
                print instance
        else:
            print "Please input a model name from:"
            for res in self.unisrt._resources:
                print res
    
    def do_la(self, args):
        '''
        Usage: la
        list names of all application in this runtime environment
        '''
        print "List of applications:"
        for _, app, _  in pkgutil.iter_modules(['apps']):
            print app
        
    
    def do_run(self, args):
        '''
        Usage: run <application name> [argument list]
        executes the application driver in the apps package
        '''
        args = args.split(' ')
        full_name = 'apps.' + args[0] + '.' + args[0]
        app = __import__(full_name, fromlist = ['run'])
        
        # refer to the comments in __init__(), as the runtime environment is implemented
        # as an object of shell, it has to be passed to applications explicitly
        try:
            threading.Thread(name=full_name, target=app.run, args=(self.unisrt, args[1], )).start()
        except IOError as e:
            print e
            
            
            
            
            
    
    def do_resume(self, args):
        
        import subprocess
        from time import sleep
        subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', '1hop'])
        sleep(5)
        subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', '2hop'])
        sleep(5)
        subprocess.call(['apps/beacon/esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', 'myhop'])
        sleep(5)
        subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', '2hop'])
        
        
        path_name = "http://dev.crest.iu.edu:8889/nodes/domain_utah.edu_node_slc-slice-perf.chpc.utah.edu%http://dev.crest.iu.edu:8889/nodes/domain_indiana.edu_node_dresci"
        path0 = filter(lambda path: path.status == 'ON', self.unisrt.paths['existing'][path_name])[0]
        path0.status = 'OFF'
        path0.healthiness = 'unknown'
        path0.performance = 'unknown'
        path0.renew_local(path_name)
        path1 = filter(lambda path: path.status == 'OFF', self.unisrt.paths['existing'][path_name])[0]
        path1.status = 'ON'
        path1.healthiness = 'good'
        path1.renew_local(path_name)
        
        self.unisrt.pushRuntime('paths')
    
    def do_quit(self, args):
        '''
        Quits the program.
        '''
        print "Quitting."
        sys.exit()
        
    def do_EOF(self, args):
        return True

def main():
    NREShell().cmdloop('Welcome to the UNIS Runtime Environment!')

if __name__ == '__main__':
    main()
