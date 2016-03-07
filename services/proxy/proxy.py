"""
as an example use case: the dlt web server may need to invoke functions from nre, however
Joseph's argument is local invocations are generally bad for security reasons.
so this local web server will communication with other web servers via restful. the
difference(s) between this restful interface and the unis' is that this interface is
for function invocations not object retrievals. 
"""
import json, re
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler

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
        this proxy only serves Joseph now, so here is what we do per his requests:
        
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
                        ret['nodes'].append(node.name)
                        for port in node.ports.values():
                            ret['ports'].append({'id': port.id, 'node': node.name})
                            
                    for measurement in self.server.unisrt.measurements['existing'].values():
                        if hasattr(measurement, 'src'):
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
        post PIT measurements
        /domain/forecast/{specs}
        
        if None != re.search('/api/v1/addrecord/*', self.path):
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'application/json':
                length = int(self.headers.getheader('content-length'))
                data = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
                recordID = self.path.split('/')[-1]
                LocalData.records[recordID] = data
                print "record %s is added successfully" % recordID
            else:
                data = {}
 
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(403)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
        '''
        return
        
class NREServer(HTTPServer):
    def set_nre(self, unisrt):
        setattr(self, 'unisrt', unisrt)
        
def run(unisrt, kwargs):
    server = NREServer(("localhost", 8080), MyRequestHandler)
    server.set_nre(unisrt)
    server.serve_forever()