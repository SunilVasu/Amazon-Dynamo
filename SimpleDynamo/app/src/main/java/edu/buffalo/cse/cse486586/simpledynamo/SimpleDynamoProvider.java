package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	String TAG = SimpleDynamoProvider.class.getSimpleName();
	private final Uri myUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	static final int SERVER_PORT = 10000;
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String[] PORTS = {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
	ArrayList<Node> list = new ArrayList<Node>();
	static  String curPort ="";
	String predPort="";
	String succPort="";
	List<String> listOfFiles= new ArrayList<String>();

	BlockingQueue<String> bq2 = new ArrayBlockingQueue<String>(1);
	String sender="";
	boolean failComplete;


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.contains("@") || selection.contains("*")){

			Log.d(TAG,"Delete @ query");

			File[] files  = getContext().getFilesDir().listFiles();

			for(File file:files){
				file.delete();
			}
		}
		else{
			try {
				if(selectionArgs==null) {
					ClientTask client_task = new ClientTask();
					Log.i(TAG, "Delete: calling :" + "@" + getPort());
					String sent = "delete#"+selection;
					client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sent);
				}
				Log.d(TAG,"Delete key: "+ selection);
				File dir =  getContext().getFilesDir();

				File f= new File(dir,selection);
				if(f.exists())
					f.delete();
			} catch (Exception e) {
				e.printStackTrace();
				Log.v(TAG,"Exception delete: "+e);
			}

		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getNeighbour(String file) throws NoSuchAlgorithmException {
		String hk = genHash(file);
		String str=null;
		for(Node nd:list){

			if((hk.compareTo(genHash(nd.getPred().getPort())) >0 && hk.compareTo(genHash(nd.getPort())) < 0)||
					(genHash(nd.getPred().getPort()).compareTo(genHash(nd.getPort()))>0 &&
							(hk.compareTo(genHash(nd.getPred().getPort()))>0 || hk.compareTo(genHash(nd.getPort()))<0))){
				str=nd.getPort()+"#"+nd.getSucc().getPort()+"#"+nd.getSucc().getSucc().getPort();
				return  str;
			}
		}
		return  str;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String file = values.getAsString("key") ;
		String record = values.getAsString("value");

		Context context = getContext();
		File dir =  getContext().getFilesDir();


		InputStream inputStream = null;




		int check=0;
		Log.i(TAG,"Insert: input key :"+file+ " value :" +record +" curr:"+curPort );
		try {
			if(record.contains(":")){
				//listOfFiles.add(file);
				File f= new File(dir,file);
				try {

					if(f.exists()){
						inputStream = context.openFileInput(file);
						String readline =  null;
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader reader = new BufferedReader(inputStreamReader);
						readline = reader.readLine();

						if(readline.split(":")[1].compareTo((record.split(":")[1]))>0){
							record = readline;
						}
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				OutputStreamWriter outputStreamWriter = new OutputStreamWriter(context.openFileOutput(file, Context.MODE_PRIVATE));
				outputStreamWriter.write(record);
				outputStreamWriter.close();

				Log.i(TAG,"Insert: After Direct Insert @"+curPort+" key:"+file+ " record:" +record );
				check=1;
			}
			else if(!record.contains(":")){
				record = record +":"+ (long)System.currentTimeMillis();

				try {

					File f= new File(dir,file);
					if(f.exists()){
						inputStream = context.openFileInput(file);
						String readline =  null;
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader reader = new BufferedReader(inputStreamReader);
						readline = reader.readLine();

						if(readline.split(":")[1].compareTo((record.split(":")[1]))>0){
							record = readline;
						}
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}


				String p = getNeighbour(file);
				Log.i(TAG, "Insert: getNeighbour:" + p);
				String[] tmp = p.split("#");
				if (check == 0 && tmp[0].equals(getPort()) || tmp[1].equals(getPort()) || tmp[2].equals(getPort())) {

					//listOfFiles.add(file);

					OutputStreamWriter outputStreamWriter = new OutputStreamWriter(context.openFileOutput(file, Context.MODE_PRIVATE));
					outputStreamWriter.write(record);
					outputStreamWriter.close();

					String token = "insert#" + file + "#" + record;
					Log.i(TAG, "Insert: done for node:" + getPort() + " file:" + file + " record:" + record);

					for(String i:tmp){
						if(!i.equals(getPort())){
							ClientTask client_task = new ClientTask();
							Log.i(TAG, "Insert: calling :" + i + "@" + getPort() + " file:" + file + " record:" + record);
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, token, String.valueOf(Integer.parseInt(i) * 2));
						}
					}
				} else if (check == 0 && !tmp[0].equals(getPort()) && !tmp[1].equals(getPort()) && !tmp[2].equals(getPort())) {

					String token = "insert#" + file + "#" + record ;

					Log.i(TAG, "Insert: calling all:" + p + "@" + getPort() + " file:" + file + " record:" + record);
					for(String i:tmp){
						ClientTask client_task = new ClientTask();
						Log.i(TAG, "Insert: calling :" + i + "@" + getPort() + " file:" + file + " record:" + record);
						client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, token, String.valueOf(Integer.parseInt(i) * 2));

					}

				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		ServerSocket serverSocket = null;
		curPort=getPort();
		failComplete=false;
		try {
			Node node1 = new Node("5554",genHash("5554"),null,null);
			Node node2 = new Node("5558",genHash("5558"),null,null);
			Node node3 = new Node("5560",genHash("5560"),null,null);
			Node node4 = new Node("5562",genHash("5562"),null,null);
			Node node5 = new Node("5556",genHash("5556"),null,null);

			node1.setPred(node5);
			node1.setSucc(node2);

			node2.setPred(node1);
			node2.setSucc(node3);

			node3.setPred(node2);
			node3.setSucc(node4);

			node4.setPred(node3);
			node4.setSucc(node5);

			node5.setPred(node4);
			node5.setSucc(node1);

			list.add(node1);
			list.add(node2);
			list.add(node3);
			list.add(node4);
			list.add(node5);

			for(Node n:list){
				Log.v(TAG, "Node :"+n.getPort());
				if(n.getPort().equals(curPort)){
					predPort=n.getPred().getPort();
					succPort=n.getSucc().getPort();
					Log.v(TAG, "curPort :"+n.getPort()+" predPort:"+predPort+" succPort:"+succPort);
				}
			}

			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "failrecovery");

		Log.v(TAG, "OnCreate: *end* flag failComplete :"+failComplete);
		Log.v(TAG, "OnCreate: ServerSocket Created :"+curPort);
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		if( (selectionArgs!=null && !selectionArgs[0].equals("fail")) || selectionArgs==null){
			try {
				//Log.v(TAG, "Query: flag failComplete :"+failComplete);
				while(!failComplete)
				{
					Thread.sleep(100);
					Log.v(TAG, "Query: *While* t_sleep failComplete:"+failComplete);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String readline =  null;
		Context context = getContext();
		MatrixCursor matCur = new MatrixCursor(new String[] {"key", "value"});


		try {
			Log.v(TAG,"query inside +=>"+curPort+" selection:"+selection);
			String[] list = getContext().fileList();

			if(selection.contains("@") || selection.contains("*")){
				for (File filePath : getContext().getFilesDir().listFiles()) {

					String filename = filePath.toString();

					String pattern = Pattern.quote(System.getProperty("file.separator"));

					String[] splitFileName = filename.split(pattern);

					String file = splitFileName[splitFileName.length-1];

					InputStream inputStream = context.openFileInput(file);
					if (inputStream != null) {

						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader reader = new BufferedReader(inputStreamReader);
						readline = reader.readLine();
						if(selectionArgs==null){
							readline=readline.split(":")[0];
						}
						Log.i(TAG,"for @:: File:"+file+"\t Value:"+readline);
						String record[] = {file, readline};
						matCur.addRow(record);
						Log.d("Count", matCur.getCount() + "");
						//return matCur;

					} else {
						Log.v("query", "inputStream is NULL");
					}
				}

				if(selection.contains("*") && !(succPort.equals("") || succPort==null)){

					String msg = "star#";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,String.valueOf(Integer.parseInt(curPort)*2));

					String keyvals = bq2.take();

					Log.d(TAG,"Query: star KeyVals Received: "+ keyvals);
					String[] pairs = keyvals.split("-");
					for(String p: pairs){

						if(!p.equals("") && !(p==null)){
							String[] kv = p.split("_");
							String key = kv[0];
							String val = kv[1].split(":")[0];
							String record[] = {key, val};

							matCur.addRow(record);

						}
					}
				}
				return matCur;
			}
			else {
				//Normal Query
				String hash = genHash(selection);
				boolean cond = (succPort.equals("") )|| (hash.compareTo(genHash(predPort)) >0 && hash.compareTo(genHash(curPort)) < 0) ||
						(genHash(predPort).compareTo(genHash(curPort))>0 && (hash.compareTo(genHash(predPort))>0 || hash.compareTo(genHash(curPort))<0));

				Log.v("query", "normal: selection:"+selection+" currNode:"+curPort);

				//case1: present in the 1st query node
				String s = getNeighbour(selection);
				String[] temp = s.split("#");
				if(selectionArgs==null && getPort().equals(temp[0]) || getPort().equals(temp[1]) || getPort().equals(temp[2])){
					Log.v("query:", "inside:"+getPort()+" p:"+s);
					InputStream inputStream = context.openFileInput(selection);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader reader = new BufferedReader(inputStreamReader);
					readline = reader.readLine();

					//String record[] = {selection, readline};
					//matCur.addRow(record);
					Log.v("query:", "returning selection:"+selection+" readline:"+readline);
					//return matCur;
				}
				//calling the coordinator node directly
				if(selectionArgs==null) {

					String st = getNeighbour(selection);
					String[] sa = st.split("#");
					String var="";
					for(String i:sa){
						if (i.equals(getPort())){
							InputStream inputStream = context.openFileInput(selection);
							InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
							BufferedReader reader = new BufferedReader(inputStreamReader);
							readline = reader.readLine();
							//readline=readline.split(":")[0];
							Log.i(TAG,"for @:: File:"+selection+"\t Value:"+readline);

							String record[] = {selection, readline};
							var = selection+"_"+readline;
							//matCur.addRow(record);
							//Log.d("Count", matCur.getCount() + "");
						}
					}

					Log.v("query", "direct call @"+curPort);
					String msg="query#"+selection+"#"+curPort;
					Log.v("query", "client msg:"+msg);
					BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, bq);


					String str = bq.take();

					if(!var.equals(""))
						str+=var;

					Log.v("Query","str:"+str);

					String[] pairs = str.split("-");

					String[] kv = pairs[0].split("_");
					String key = kv[0];
					String val = kv[1].split(":")[0];
					String ts = kv[1].split(":")[1];

					String[] kv2 = pairs[1].split("_");
					String val2 = kv2[1].split(":")[0];
					String ts2 = kv2[1].split(":")[1];

					if(ts2.compareTo(ts)>0){
						val = val2;
					}

					String record[] = {key, val};
					matCur.addRow(record);
					return matCur;

				} else if(selectionArgs!=null){
					Log.v("query:", "selectionArgs not null: inside:"+getPort()+" p:"+s);
					InputStream inputStream = context.openFileInput(selection);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader reader = new BufferedReader(inputStreamReader);
					readline = reader.readLine();

					String record[] = {selection, readline};
					matCur.addRow(record);
					Log.v("query:", "returning selection:"+selection+" readline:"+readline);
					return matCur;
				}

			}



		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	//ServerTask
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];


			while (true) {
				try {
					Socket socket = serverSocket.accept();
					socket.setSoTimeout(150);
					Log.d(TAG,"Server: Testing @ "+curPort);

					DataOutputStream dataOut;
					BufferedReader dataIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));

					String msg2 = dataIn.readLine();
					Log.d(TAG,"Server: Entered srv task:" +msg2);

					if(msg2.startsWith("delete")){
						String sel = msg2.split("#")[1];
						String[] rand = {"rand"};
						delete(myUri,sel,rand);
						dataOut = new DataOutputStream(socket.getOutputStream());
						dataOut.writeBytes("deleteack"+"\n");
						dataOut.flush();
						dataOut.close();
						dataIn.close();
						socket.close();

					}
					else if(msg2.startsWith("failrecovery")){
						String[] in = msg2.split("#");
						String port = in[1];
						String ack="failack#";
						String[] t = {"fail"};
						Cursor cursor = query(myUri, null, "@" ,t , null, null);
						if(cursor.moveToFirst()){
							do{

								String key = cursor.getString(cursor.getColumnIndex("key"));
								String value = cursor.getString(cursor.getColumnIndex("value"));
								ack += key +"_" + value+"-";

							}while (cursor.moveToNext());
						}

						dataOut = new DataOutputStream(socket.getOutputStream());
						dataOut.writeBytes(ack+"\n");
						dataOut.flush();
						dataOut.close();
						dataIn.close();
						socket.close();

					}else if(msg2.startsWith("insert")){
						//"insert#"+file+"#"+record;
						String[] in = msg2.split("#");
						ContentValues values= new ContentValues();

						values.put("key",in[1]);
						values.put("value",in[2]);

						dataOut = new DataOutputStream(socket.getOutputStream());
						dataOut.writeBytes("insertack\n");
						dataOut.flush();
						dataOut.close();
						dataIn.close();
						socket.close();

						insert(myUri,values);

					} else if(msg2.startsWith("query")) {
						try{
							//"query#"+selection+"#"+succPort+"#"+origPort;
							Log.i(TAG, "Server:for query msg:" + msg2);
							String[] in = msg2.split("#");

							String[] arr = {"rand"};
							Cursor cur = query(myUri, null, in[1], arr, null, null);

							String key = null;
							String value = null;

							if (cur.moveToFirst()) {
								do {

									key = cur.getString(cur.getColumnIndex("key"));
									value = cur.getString(cur.getColumnIndex("value"));

								} while (cur.moveToNext());
							}

							Log.i(TAG, "Server:for query msg:" + msg2 + " value:" + value);
							dataOut = new DataOutputStream(socket.getOutputStream());
							dataOut.writeBytes("queryack#" + key + "_" + value + "\n");
							dataOut.flush();
							dataOut.close();
							dataIn.close();
							socket.close();
						}catch (Exception e){
							Log.i(TAG, "Query Exception :"+e);
						}
					}else if(msg2.startsWith("star")){

						Log.d(TAG,"Star Query Received at: "+ getPort());
						Cursor cursor = query(myUri, null, "@" , null, null, null);
						String ack = "starack#";
						if(cursor.moveToFirst()){
							do{

								String key = cursor.getString(cursor.getColumnIndex("key"));
								String value = cursor.getString(cursor.getColumnIndex("value"));
								ack += key +"_" + value+"-";

							}while (cursor.moveToNext());
						}

						ack += "#"+getPort();

						dataOut = new DataOutputStream(socket.getOutputStream());
						dataOut.writeBytes(ack+"\n");
						dataOut.flush();
						dataOut.close();
						dataIn.close();
						socket.close();
					}
				} catch (SocketException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}


	private class ClientTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... msgs) {

			try {

				String msgToSend = (String) msgs[0];

				boolean check = true;
				//Send a message over the socket
				Log.i(TAG, "Client: msgToSend: " + msgToSend);

				if(msgToSend.startsWith("delete")) {
					try {

						String[] sel = msgToSend.split("#");
						String n = getNeighbour(sel[1]);

						for(String t: n.split("#")) {
							try{
								if (!t.equals(getPort())) {
									Socket socket8 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(t) * 2);
									DataOutputStream dataOut8 = new DataOutputStream(socket8.getOutputStream());

									Log.i(TAG, "Client: delete:" + msgToSend);
									dataOut8.writeBytes(msgToSend + "\n");
									dataOut8.flush();

									BufferedReader din8 = new BufferedReader(new InputStreamReader(socket8.getInputStream()));
									String ack1 = din8.readLine();
									Log.i(TAG, "Client: for delete ack:" + ack1);
									if (ack1.startsWith("deleteack")) {
										Log.i(TAG, "Client:Received ack delete:" + ack1);
										din8.close();
										socket8.close();

									}
								}
							}catch (Exception e){
								Log.v("Client","Delete Exception isndie"+e);
							}
						}




					}catch (Exception e){
						Log.v("Client","Delete Exception"+e);
					}
				}
				else if(msgToSend.startsWith("failrecovery")) {
					String total = "";
					String t = "";
					for (Node n : list) {
						try {
							Log.v(TAG, "failrecovery currNode :" + getPort());
							if (!n.getPort().equals(getPort())) {
								Log.v(TAG, "failrecovery calling Node :" + n.getPort());
								Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(n.getPort()) * 2);
								DataOutputStream dataOut4 = new DataOutputStream(socket4.getOutputStream());
								t = "failrecovery#" + getPort();
								Log.i(TAG, "Client: msg:" + t);
								dataOut4.writeBytes(t + "\n");
								dataOut4.flush();

								BufferedReader din4 = new BufferedReader(new InputStreamReader(socket4.getInputStream()));
								String ack1 = din4.readLine();
								Log.i(TAG, "Client: for fail ack:" + ack1);
								if (ack1.startsWith("failack")) {
									Log.i(TAG, "Client:Received ack failrecovery:" + ack1);
									din4.close();
									socket4.close();
									String[] temp = ack1.split("#");
									if (temp.length > 1)
										total += ack1.split("#")[1];
								}

							}
						} catch (Exception e) {
							Log.v("Client", "Efailure exception" + e);
						}
					}
					Log.v(TAG, "Client: failrecovery total :" + total);

					if (total.length() > 0)
					{
						Log.v(TAG, "Client: failrecovery total len:" + total.length());
						String[] p = total.split("-");

					HashMap<String, String> map = new HashMap<String, String>();

					for (String pr : p) {
						String[] kv = pr.split("_");
						String k = kv[0];
						String v = kv[1];
						String[] rVal = getNeighbour(k).split("#");
						for (String s : rVal) {
							if (s.equals(getPort())) {
								if (!map.containsKey(k)) {
									map.put(k, v);
								} else {
									if (v.split(":")[1].compareTo(map.get(k).split(":")[1]) > 0) {
										map.put(k, v);
									}
								}
							}
						}


					}

						for(String key:map.keySet()){
							ContentValues values= new ContentValues();
							values.put("key",key);
							values.put("value",map.get(key));
							insert(myUri,values);
						}
				}

					failComplete = true;
					Log.i(TAG, "Client: failRecovery done, setting failComplete:"+failComplete);
				}
				else if (msgToSend.startsWith("insert")) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt((String) msgs[1]));
						DataOutputStream dataOut1 = new DataOutputStream(socket.getOutputStream());
						Log.i(TAG, "Client:sending to server:" + Integer.parseInt((String) msgs[1]) / 2 + " :" + msgToSend);
						dataOut1.writeBytes(msgToSend + "\n");
						dataOut1.flush();

						BufferedReader din = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String ack1 = din.readLine();
						Log.i(TAG, "Client:After update from:" + Integer.parseInt((String) msgs[1]) / 2 + " :" + ack1);
						if (ack1 == null)
							throw new NullPointerException();
						if (ack1.equals("insertack")) {
							Log.i(TAG, "Client:Received ack for insert:" + ack1);
							din.close();
							socket.close();
						}
					}catch (Exception e){
						Log.v(TAG,"Exception in Insert: "+e);
					}


				} else if (msgToSend.startsWith("query")){
					// "query#"+selection+"#"+temp[0]+temp[1]+temp[2]+"#"+curPort;
					int resp_counter=0;
					String kvstr="";
					String[] msg = msgToSend.split("#");
					BlockingQueue<String> bq3 = (BlockingQueue<String>) msgs[1];
					Log.i(TAG, "msg at client for query: " + msgToSend);
					ArrayList<String> node = new ArrayList<String>();

					String m = getNeighbour(msg[1]);

					node.add(m.split("#")[0]);
					node.add(m.split("#")[1]);
					node.add(m.split("#")[2]);

					if(node.contains(getPort())){
						node.remove(getPort());
						resp_counter = 1;
					}


					for(String n:node){
						try {


							Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(n) * 2);
							DataOutputStream dataOut2 = new DataOutputStream(socket2.getOutputStream());

							dataOut2.writeBytes(msgToSend + "\n");
							dataOut2.flush();

							BufferedReader dataIn2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
							String ack2 = dataIn2.readLine();
							Log.i(TAG, "queryack: " + ack2);

							if (ack2 == null)
								throw new NullPointerException();

							if (ack2.startsWith("queryack")) {
								String[] in = ack2.split("#");
								String[] s = in[1].split("_");


								kvstr += in[1]+"-";
								resp_counter++;
								if(resp_counter==2){
									bq3.put(kvstr);
								}

								Log.i(TAG, "Client:Received ack for query:" + ack2 + " from:" + msg[2]);
								dataIn2.close();
								socket2.close();
							}

						}catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}catch (Exception e){
							Log.v("Query","Query OuterException: "+e);
						}
					}




				}

				else if (msgToSend.startsWith("star")) {
					//"star#" + selection + "#" + succPort + "#" + orgPort;

					Log.i(TAG, "msg at client for star: " + msgToSend);

					String sendToPort = succPort;
					String originalSender = curPort;
					String pairs = "";

					for(Node p:list) {
						try {
							Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p.getPort()) * 2);
							DataOutputStream dataOut2 = new DataOutputStream(socket3.getOutputStream());

							dataOut2.writeBytes(msgToSend + "\n");
							dataOut2.flush();

							BufferedReader dataIn2 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
							String ack2 = dataIn2.readLine();
							Log.i(TAG, "Client:starack: " + ack2);

							if (ack2 == null)
								throw new NullPointerException();

							if (ack2.startsWith("starack")) {
								//Log.i(TAG, "Client:Received ack for star:" + ack2 + " from:" + msg[2]);
								String[] data = ack2.split("#");

								Log.d(TAG, "Client: KeyVal Received from " + sendToPort + ": Sending To :" + data[2].trim() + " " + data[1]);



								pairs += data[1];

								dataIn2.close();
								socket3.close();
							}
						}catch(Exception e){
							Log.v(TAG,"star Exception"+e);
						}
					}
					bq2.put(pairs);
				}

			} catch (Exception e){
				Log.v(TAG,"here in Client Exception:"+e);
			}

			return null;
		}
	}




	//Reference PA2
	//eg:11108
	private String getPortNum() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String portNum = String.valueOf((Integer.parseInt(portStr) * 2));;
		return portNum;
	}
	//eg:5554
	private String getPort() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String portNum = String.valueOf((Integer.parseInt(portStr) ));;
		return portNum;
	}

	public class Node {
		public String port;
		public String Node_id;
		public Node pred;
		public Node succ;

		public Node(String portNum, String Node_id, Node pred, Node succ){
			this.port = portNum;
			this.Node_id = Node_id;
			this.pred = pred;
			this.succ = succ;
		}

		public void setPort(String port){
			this.port = port;
		}
		public void setNode_id(String Node_id){
			this.Node_id = Node_id;
		}
		public void setPred(Node pred){
			this.pred = pred;
		}
		public void setSucc(Node succ){
			this.succ = succ;
		}


		public String getPort(){
			return this.port;
		}
		public String getNode_id(){
			return this.Node_id;
		}
		public Node getPred(){
			return this.pred;
		}
		public Node getSucc(){
			return this.succ;
		}

	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}