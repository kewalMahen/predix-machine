

Hoover Sample - Shows how to provides the sample for a Processor 
implementation which will process the data as per configuration on 
the spillway.  Follow the steps below:

This sample requires encyption features, copy the security files, i.e.
security/com.ge.dspmicro.securityadmin.cfg 

Note: the com.ge.dspmicro.securityadmin.cfg will set the client certificates and enable http browsing. See properties:
    org.apache.felix.http.enable=true
    org.apache.felix.https.clientcertificate=needs
    
    If you add web/technician console support for testing, the browser needs to go through the http address and not the https because of the client certificates.
    I.e. http://localhost:8181/system/console

