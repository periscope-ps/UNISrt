#!/usr/bin/env python

import cmd
import sys
import threading
import pkgutil
from graph_tool.all import Graph, graph_draw

import kernel.unisrt
from libnre.utils import *

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
            
    def do_dt(self, layer):
        '''
        draw topology of certain layer, L2 by default
        Usage: dt <layer number>
        '''
        if not layer or int(layer) == 2:
            graph_draw(self.unisrt.g)
        elif int(layer) == 3:
            # prepare a layer 3 graph and then draw it
            # the graph could be constructed in advance
            g = Graph()
            nodebook = {}
            tmp = self.unisrt.ipports['existing'].values()
            for index1, ipp1 in enumerate(tmp):
                for index2, ipp2 in enumerate(tmp):
                    if ipp1 is ipp2: continue
                    if issamesubnet(ipp1.address, ipp2.address, '255.255.255.0'):
                        if ipp1.node.selfRef not in nodebook:
                            nodebook[ipp1.node.selfRef] = g.add_vertex()
                        if ipp2.node.selfRef not in nodebook:
                            nodebook[ipp2.node.selfRef] = g.add_vertex()
                        g.add_edge(nodebook[ipp1.node.selfRef], nodebook[ipp2.node.selfRef], add_missing=False)
                        g.add_edge(nodebook[ipp2.node.selfRef], nodebook[ipp1.node.selfRef], add_missing=False)
                        tmp.pop(index2)
                        
            graph_draw(g)
        else:
            print "only draw layer 2 or 3"
        
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
            
    def do_service(self, args):
        '''
        Usage: service <service name> [argument list]
        start a nre service -- shouldn't be exactly the same as running an app
        '''
        args = args.split(',')
        args = map(lambda x: x.replace(' ', ''), args)
        full_name = 'services.' + args[0] + '.' + args[0]
        app = __import__(full_name, fromlist = ['service'])
        
        parameters = {}
        for index, arg in enumerate(args):
            if index == 0: continue
            kv = arg.split('=')
            parameters[kv[0]] = kv[1]
        
        # refer to the comments in __init__(), as the runtime environment is implemented
        # as an object of shell, it has to be passed to applications explicitly
        try:
            threading.Thread(name=full_name, target=app.run, args=(self.unisrt, parameters, )).start()
        except IOError as e:
            print e
            
    def do_pitall(self, args):
        '''
        a command to schedule full mesh traceroute and iperf tests
        '''
        import commands.pitall as ping_iperf_traceroute_all
        ping_iperf_traceroute_all.work(self.unisrt)
        
    def do_forecast(self, args):
        args = args.split(',')
        args = map(lambda x: x.replace(' ', ''), args)
        pair = (args[0], args[1])
        tolerance = float(args[2])
        result = self.unisrt.forecaster.forecast(pair, tolerance)
        print result
    
    def do_flange(self, args):
        '''
        it is the compiler, which takes in texts written in Flange and generates
        nre programs like the ones in app/ directory
        '''
        pass
            
    def do_resume(self, args):
        '''
        temporary function to reverse the SC15 demo testbed
        '''
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

if __name__ == '__main__':
    NREShell().cmdloop('Welcome to the UNIS Runtime Environment!')
