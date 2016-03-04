package com.hung.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class HungServlet extends HttpServlet{
	private static final long serialVersionUID = 1L;

	protected AerospikeClient client;
	
	private boolean initConnection(HttpServletRequest request, HttpServletResponse response) throws IOException {
		ClientPolicy policy = new ClientPolicy();
		client = new AerospikeClient(policy, "localhost", 3000);						
		
		if (client.isConnected())
			return true;		
		return false;
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		
		if(initConnection(request, response)) {
			String pathInfo = request.getPathInfo();
			
			pathInfo = stripChar(pathInfo, '/');
			String[] pathArray = pathInfo.split("/");
			
	        String namespace = pathArray[0];
	        String set       = pathArray[1];
	        String userKey   = pathArray[2];
	        
	        Key key = new Key(namespace, set, userKey);
	        
	        String bin = request.getParameter("bin");
	        String val = request.getParameter("val");
	        
	        WritePolicy writePolicy = new WritePolicy();
	        writePolicy.timeout = 0;
	        writePolicy.expiration = 0;
	        writePolicy.sendKey = true;
	        
	        Bin[] binArray = new Bin[1];
	        binArray[0] = new Bin(bin, val);
	        client.put(writePolicy, key, binArray);
	        
	        out.println("Success!");
		}
		else {
			out.println("Failed!");
		}
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
		initConnection(request, response);
		
		response.setHeader("Access-Control-Allow-Origin", "*");
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache");

        @SuppressWarnings("unchecked")
		Map<String, String[]> params = request.getParameterMap();
        String pathInfo = request.getPathInfo();

        final Writer writer = response.getWriter();
        
        // If no pathInfo, just bail now.
        if (pathInfo == null) {
            response.setStatus(HttpServletResponse.SC_OK);
            writer.flush();
            return;
        }
                
        pathInfo = stripChar(pathInfo, '/');
		String[] pathArray = pathInfo.split("/");

        String namespace = pathArray[0];
        String set       = pathArray[1];
        String userKey   = pathArray[2];
        
        Key key = new Key(namespace, set, userKey);

        Policy policy = new Policy();
        policy.timeout = 0;            
        
        Record record;
        record = this.client.get(policy, key);
        
        if (record.bins != null) {
            Set<?> keyset = record.bins.keySet();
            Iterator<?> iterator = keyset.iterator();
            if (!iterator.hasNext()) {
                try {
					throw new ResourceNotFoundException("Server Returns No Values");
				} catch (ResourceNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }

            String resultJSON = "{\"result\" : {";
            while (iterator.hasNext()) {
                Object binName = iterator.next();
                Object binValue = record.bins.get(binName);
                resultJSON += "\"" + binName.toString() + "\" : ";
                if( binValue instanceof Integer ) {
                    resultJSON += binValue.toString();
                } else if (binValue instanceof byte[]) {
                    System.out.println(" it's a blob!");
                } else if (binValue instanceof String) {
                    resultJSON += "\"" +  binValue.toString() + "\"";
                } else {
                    resultJSON += "\"\"";
                }
                if (iterator.hasNext()) {
                    resultJSON += ",";
                }
            }
            resultJSON += "},";
            resultJSON += "\"generation\" : " + record.generation + "}";
            writer.write(resultJSON);
        }
	}
	
	private static String stripChar(String inStr, char stripChar) {
        int i = 0;
        for (i = 0; i < inStr.length(); i++) {
            if (inStr.charAt(i) != stripChar) {
                break;
            }
        }
        if (i == inStr.length())
            return inStr;
        else
            return inStr.substring(i, inStr.length());
    }
	
	@Override
	public void destroy() {
		if (client != null) {
			client.close();
			client = null;
		}
	}
	
	private class ResourceNotFoundException extends Exception {
		private static final long serialVersionUID = 1L;
	
		public ResourceNotFoundException(String message) {
			super(message);
		}
	}
}
