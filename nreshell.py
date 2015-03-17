'''
Created on Feb 12, 2015

@author: mzhang
'''
import cmd
import kernel.unisrt

class NREShell(cmd.Cmd):
    
    def __init__(self):
        # before I could ever implement the runtime environment as a separate
        # stand alone daemon, it'd be an object of the shell
        self.unisrt = kernel.unisrt.UNISrt()        
        
        self.prompt = '> '
        cmd.Cmd.__init__(self)
        
    def do_lm(self, model):
        '''
        Usage: lm [model name]
        list objects of requested model
        if no model specified, list all of them
        '''
        print self.unisrt.measurements
        print self.unisrt
    
    def do_la(self, app):
        '''
        Usage: la
        list names of all application in this runtime environment
        '''
        pass
    
    def do_run(self, app, args='/home/mzhang/workspace/nre/apps/faultlocator/faultlocator.conf'):
        '''
        Usage: run <application name> [argument list]
        executes the application driver in the apps package
        '''
        appname = 'apps.' + app
        app = __import__(appname, fromlist = ['run'])
        
        # refer to the comments in __init__(), as the runtime environment is implemented
        # as an object of shell, it has to be passed to applications explicitly
        app.run(self.unisrt, args)

    def do_quit(self, args):
        '''
        Quits the program.
        '''
        print "Quitting."
        raise SystemExit

def main():
    nreshell = NREShell()
    nreshell.cmdloop('Welcome to the UNIS Runtime Environment!')

if __name__ == '__main__':
    main()
