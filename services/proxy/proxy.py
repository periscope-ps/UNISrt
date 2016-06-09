"""
as an example use case: the dlt web server may need to invoke functions from nre, however
Joseph's argument is local invocations are generally bad for security reasons.
so this local web server will communication with other web servers via restful. the
difference(s) between this restful interface and the unis' is that this interface is
for function invocations not object retrievals. 
"""
import json, re
import cgi
import settings
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler

logger = settings.get_logger('proxy')

class MyRequestHandler (BaseHTTPRequestHandler) :
    
    def get_intf_id(self, domain, ipv4):
        for node in domain.nodes:
            for port in node.ports.values():
                # cheated before models are improved
                try:
                    if port.data['properties']['ipv4']['address'] == ipv4:
                        return port.id
                except KeyError:
                    continue
    
    def do_GET(self):
        '''
        /domain-id
        {
        nodes: [<name>, ...],
        ports: [{id: <port-id>, node: <name>}, ...],
        measurements: [{node: <name>, measurement: <measurement-id>}...],
        or
        measurements: [{src: <port-id>, dest: <port-id>, measurement: <measurement-id>}...]
        }
        '''
        if re.search('/*', self.path) != None:
            query = self.path.split('/')
            
            if query[1] == 'domains':
                ret = self.server.unisrt.domains['existing'].keys()
                json.dump(ret, self.wfile)
                return
            
            the_domain = filter(lambda x: re.match('.*_' + query[1] + '$', x.id), self.server.unisrt.domains['existing'].values())
            if not the_domain:
                self.send_response(400, 'Bad Request: domain does not exist')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
            elif len(the_domain) == 1:
                the_domain = the_domain[0]
                if not len(query) > 2 or (len(query) == 3 and query[-1] == ''): 
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    
                    ret = {'nodes': [], 'ports': [], 'measurements': []}
                    for node in the_domain.nodes:
                        ret['nodes'].append({'domain': the_domain.id, 'name': node.name})
                        for port in node.ports.values():
                            if 'properties' in port.data and 'ipv4' in port.data['properties'] and 'address' in port.data['properties']['ipv4']:
                                ret['ports'].append({'id': port.id, 'node': node.name, 'ipv4': port.data['properties']['ipv4']['address']})
                            
                    for measurement in self.server.unisrt.measurements['existing'].values():
                        if hasattr(measurement, 'src') and self.get_intf_id(the_domain, measurement.src):
                            ret['measurements'].append({'src': self.get_intf_id(the_domain, measurement.src),\
                                'dest': self.get_intf_id(the_domain, measurement.dst), 'measurement': measurement.id})
                    
                    json.dump(ret, self.wfile)
                else:
                    json.dump({'error': 'not implemented yet'}, self.wfile)
            else:
                self.wfile.write('multiple domains match the query?')
        else:
            self.send_response(403)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()

        return
            
    def do_POST(self):
        '''
        post atomic PIT measurement objects:
        {
            domain: <domain-id>,
            node: <node-id>,
            port: <port-id>,
            address: <IPv4 address>
        }
        '''
        if not hasattr(self.server.unisrt, 'forecaster'):
            logger.warn("NRE service forecaster needs to start first")
            return
        
        if re.search('/fullmesh', self.path) != None:
            # expects [<atom object>...]
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'application/perfsonar+json':
                length = int(self.headers.getheader('content-length'))
                data = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
                data = json.loads(data.keys()[0]) # TODO: how this is parsed?
                
                # full mesh: pair up and submit the task batch to service forecaster
                targets = []
                for i in data:
                    for j in data:
                        if i['port'] == j['port']:
                            # assume port-id is globally unique
                            continue
                        targets.append({'src-domain': i['domain'], 'src-node': i['node'],\
                                        'src-addr': i['address'], 'dst-addr': j['address']})
                        
                new_measurement_ids = self.server.unisrt.forecaster.follow(targets, ['iperf', 'owping'],\
                    scheduler="builtins.scheduled", schedule_params={'every': 1800, 'duration': 30, 'num_tests': 24})
                for meas_id in new_measurement_ids:
                    self.server.unisrt.forecaster.forecast(meas_id, persistent=True)
                    
                self.send_response(200)
                self.end_headers()
            else:
                data = {}
                self.send_response(400)
                self.end_headers()
            
        elif re.search('/update', self.path) != None:
            # {delete: [measurement-id*], insert: [[end, end]*]}
            # execute all deletes before any inserts as a two-step way to achieve update operations
            # if any part of the request cannot be satisfied the whole thing should fail and return a 400-series error code
            # (and maybe a message saying what caused the failure), otherwise return 200.
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'application/perfsonar+json':
                length = int(self.headers.getheader('content-length'))
                data = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
                data = json.loads(data.keys()[0]) # TODO: how this is parsed?
                
                if 'delete' in data:
                    del_meas = data['delete']
                    if not self.server.unisrt.forecaster.unfollow(del_meas):
                        data = {}
                        self.send_response(400)
                        self.end_headers()
                        return
                
                if 'insert' in data:
                    targets = []
                    for ends in data['insert']:
                        targets.append({'src-domain': ends[0]['domain'], 'src-node': self.server.unisrt.unis_url+'/nodes/'+ends[0]['node'],\
                                        'src-addr': ends[0]['address'], 'dst-addr': ends[1]['address']})
                        targets.append({'src-domain': ends[1]['domain'], 'src-node': self.server.unisrt.unis_url+'/nodes/'+ends[1]['node'],\
                                        'src-addr': ends[1]['address'], 'dst-addr': ends[0]['address']})
                    
                    if not self.server.unisrt.forecaster.follow(targets,\
                                                                ['iperf', 'owping'],\
                                                                scheduler="builtins.scheduled",\
                                                                schedule_params={'every': 1800, 'duration': 30, 'num_tests': 1}):
                        data = {}
                        self.send_response(400)
                        self.end_headers()
                        return
                
                self.send_response(200)
                self.end_headers()
            else:
                data = {}
                self.send_response(400)
                self.end_headers()
        else:
            self.send_response(403)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
        
        return
        
class NREServer(HTTPServer):
    def set_nre(self, unisrt):
        setattr(self, 'unisrt', unisrt)
        
def run(unisrt, kwargs):
    server = NREServer(("localhost", 8080), MyRequestHandler)
    server.set_nre(unisrt)
    server.serve_forever()