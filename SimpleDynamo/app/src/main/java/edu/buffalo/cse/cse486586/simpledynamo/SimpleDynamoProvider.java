package edu.buffalo.cse.cse486586.simpledynamo;
// ===================================================== Author: Sricharan Anand (sanand3) ===============================================================//
// *******************************************************************************************************************************************************//
// *******************************************************************************************************************************************************//
// *******************************************************************************************************************************************************//
// *******************************************************************************************************************************************************//
// *******************************************************************************************************************************************************//
// *****************************************************       REFERENCES ********************************************************************************//

// 1. https://stackoverflow.com/questions/39571253/sqlite-table-constraint-unique-and-on-conflict-replace-usage
// 2. https://www.tutorialspoint.com/android/android_sqlite_database.htm
// 3. https://developer.android.com/guide/topics/providers/content-provider-creating.html#java
// 4. Android Documentation
// 5. http://myandroidnote.blogspot.com/2011/11/at-last-i-concluded-it-is.html

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.SimpleCursorAdapter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDynamoProvider extends ContentProvider {



	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	Uri mUri=buildURI("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	LinkedList<String> avd = new LinkedList<String>();
	LinkedList<String> avdports;
	LinkedList<String> failedavd = new LinkedList<String>();


	static final int SERVER_PORT = 10000;
	static final String[] REMOTE_PORT = {"11108", "11112", "11116","11120","11124"};
	static final String Port_0="5554";
	static final String Port_1="5556";
	static final String Port_2="5558";
	static final String Port_3="5560";
	static final String Port_4="5562";
	String myPort;
	String failedPort;
	String newFailedPort;
	String key_val_versions;
	String timestamp;

	private SQLiteDatabase db;
	private SQLiteDatabase rdb;
	private SQLiteDatabase qdb;
	private  SQLiteDatabase mdb;
	private DatabaseHelper dbh;

	Lock lock=new ReentrantLock();
	ArrayList<String> key_val_t=new ArrayList<String>();




	private Uri buildURI(String scheme, String auth)
	{
		Uri.Builder uriBuilder=new Uri.Builder();
		uriBuilder.scheme(scheme);
		uriBuilder.authority(auth);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		db=dbh.getWritableDatabase();
		db.delete(DatabaseHelper.TABLE_NAME,null,null);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public  class  MyComparator implements Comparator<String>{

		@Override
		public int compare(String lhs, String rhs) {
			try{
				int p1=Integer.valueOf(lhs)/2;
				int p2=Integer.valueOf(rhs)/2;

				return (genHash(Integer.toString(p1)).compareTo(genHash(Integer.toString(p2))));
			}catch (NoSuchAlgorithmException nsa)
			{
				nsa.getStackTrace();
			}
			return 0;
		}
	}

	public ArrayList<String> setVersioning(String[] key_val)
	{
		ArrayList<String> keyValPairs=new ArrayList<String>();
		for(String x: key_val)
		{
			String[] split;
			split=x.split("#");
			String _key=split[0];
			String port=split[1];
			int _version=1;
			if(_key.equals(key_val_t))
			{
				_version++;
				try {
				Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.valueOf(port));

					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					dataOutputStream.writeUTF(key_val+":"+_version);
					keyValPairs.add(key_val+":"+_version);
					dataInputStream.readUTF();
				}catch (IOException io)
				{
					Log.e("WRONG VERSION","WITH KEY");
					io.getStackTrace();
				}
			}

		}
		return keyValPairs;
	}

	public  class  MyKeyVersion implements Comparator<String>{

		@Override
		public int compare(String lhs, String rhs) {
			try{

				String[] split,split1;
				int version1;
				int version2;
				split=lhs.split(":");
				split1=rhs.split(":");
				version1=Integer.valueOf(split[2]);
				version2=Integer.valueOf(split1[2]);
				if(Integer.valueOf(split[2])>Integer.valueOf(split[2]))
					return version1;
				else
					return version2;

			}catch (Exception nsa)
			{
				nsa.getStackTrace();
				return 0;
			}

		}
	}

	public String failure(String input)
	{
		int char1=input.indexOf("!");
		int char2=input.indexOf("$");
		String failed="";
		int index=0;
		while(index<input.length())
		{
			if(char1>=0)
			{
				String[] split;
				split= input.split(":");
				String _key=split[0];
				String _value=split[1];
				ContentValues cv=new ContentValues();
				cv.put(DatabaseHelper.COLUMN_NAME_TITLE, _key);
				cv.put(DatabaseHelper.COLUMN_NAME_SUBTITLE, _value);
				insert(mUri, cv);
			}
		}
		return input;
	}

    public String failureQuery(String input, String input1)
    {
        int char1=input.indexOf("@");
        int char2=input.indexOf("#");
        String failed="";
        Cursor c=null;
        int index=0;
        while(index<input.length())
        {
            if(char1>=0)
            {
                String[] split;
                split= input.split(":");
                String _key=split[0];
                String _value=split[1];
                ContentValues cv=new ContentValues();
                c=query(mUri, null,null,null,null);
                while(!c.isAfterLast())
                {
                    c.moveToNext();
                    c.close();
                }
            }
        }
        return input;
    }
    public String failureInsert(String input)
    {
        int char1=input.indexOf("$");
        int char2=input.indexOf("%");
        String failed="";
        int index=0;
        while(index<input.length())
        {
            if(char1>=0)
            {
                String[] split;
                split= input.split(":");
                String _key=split[0];
                String _value=split[1];
                ContentValues cv=new ContentValues();
                cv.put(DatabaseHelper.COLUMN_NAME_TITLE, _key);
                cv.put(DatabaseHelper.COLUMN_NAME_SUBTITLE, _value);
                insert(mUri, cv);
            }
        }
        return input;
    }

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub



			String key_id;
			int port = Integer.valueOf(myPort) / 2;
			// get writable Database for inserting
			db = dbh.getWritableDatabase();

			// ========== String key to be inserted ============//
			String key = values.getAsString("key");


			Log.e("TO BE INSERETED", key);
			Log.d("Myport===", myPort);

			try {
			    // Hasing key for checking position to be inserted in
				key_id = genHash(key);
				Log.d("KEY ID== ", key_id);



				// ================ To Check if it is within the ring ======================//

				int rangeFirst = key_id.compareTo(avd.get(0));
				int rangeLast = key_id.compareTo(avd.get(avdports.size() - 1));

				if (rangeFirst <= 0 || rangeLast > 0) {
					String _key = values.getAsString(DatabaseHelper.COLUMN_NAME_TITLE);
					String _value = values.getAsString(DatabaseHelper.COLUMN_NAME_SUBTITLE);

					int lenWithout= _value.length()-1;

					if (_value.charAt(lenWithout)!='^')
					{
						String clientForward = _key + ":" + _value + "^";
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, clientForward, myPort);
					}
					else {
						// Removing special character before inserting into DB
						Log.d("Value before", _value);

						_value = _value.replace("^", "");
						Log.e("Val-toBe", _value);
						ContentValues cv = new ContentValues();
						cv.put(DatabaseHelper.COLUMN_NAME_TITLE, _key);
						cv.put(DatabaseHelper.COLUMN_NAME_SUBTITLE, _value);
						db.insert(DatabaseHelper.TABLE_NAME, null, cv);
						Log.e("INSERTED==", values.toString());

					}

				}
				//================ Key lies in the region  =============//
				else {
					String _key = values.getAsString(DatabaseHelper.COLUMN_NAME_TITLE);
					String _value = values.getAsString(DatabaseHelper.COLUMN_NAME_SUBTITLE);

					int lenWithout= _value.length()-1;

					// === Does not lie in range - forward to correct nodes (Current node + replicas)
					if (_value.charAt(lenWithout)!='<')
					{

						String clientForward = _key + ":" + _value + ">" + calculate_Node(key_id);
						Log.d("V//Forw", clientForward);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, clientForward, myPort);

					}
					else {

						// Removing delimiter character before inserting into DB
						Log.d("Value before", _value);

						_value = _value.replace("<", "");
						Log.e("Val-toBe", _value);
						ContentValues cv = new ContentValues();
						cv.put(DatabaseHelper.COLUMN_NAME_TITLE, _key);
						cv.put(DatabaseHelper.COLUMN_NAME_SUBTITLE, _value);
						db.insert(DatabaseHelper.TABLE_NAME, null, cv);
						Log.e("INSERTED==", values.toString());
					}
				}


			} catch (NoSuchAlgorithmException nsa) {
				nsa.getStackTrace();
			}


		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		String msg="";
		dbh=new DatabaseHelper(getContext());
		TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		try {
			String avd0 = genHash(Port_0);
			String avd1 = genHash(Port_1);
			String avd2 = genHash(Port_2);
			String avd3 = genHash(Port_3);
			String avd4 = genHash(Port_4);

            // Adding all hashs of ports
			avd.add(avd0);
			avd.add(avd1);
			avd.add(avd2);
			avd.add(avd3);
			avd.add(avd4);

			avdports=new LinkedList<String>();

			avdports.addAll(Arrays.asList(REMOTE_PORT));

            // Sorting to form ring
			Collections.sort(avdports, new MyComparator());
			Log.i("Sorted ports\\",avdports.toString());
			Collections.sort(avd);
			Log.e("Sorted nodes",avd.toString());
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		}catch (NoSuchAlgorithmException nsa){
			nsa.getStackTrace();
		}catch (IOException io) {
			io.getStackTrace();
		}

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "AVD alive" , myPort);
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

        // Get readable database to query the table.
		rdb=dbh.getReadableDatabase();
		SQLiteQueryBuilder sqb=new SQLiteQueryBuilder();
		sqb.setTables(DatabaseHelper.TABLE_NAME);
		Cursor c;

		int selection_length= selection.length()-1;

		// ==================== Query for @ ==================================//
		if(selection.equals("@"))
		{
			Log.d("@ Here","-->");
			// Get all key_value pairs from local AVD's
			String sql="Select * from "+ DatabaseHelper.TABLE_NAME;

			c=rdb.rawQuery(sql, selectionArgs);
			Log.d("Cursor count", String.valueOf(c.getCount()));
		}

		// ===================== Query for * ================================//
		else if(selection.equals("*"))
		{
			String sql="Select * from "+DatabaseHelper.TABLE_NAME;
			c=sqb.query(rdb, projection,null,selectionArgs,null,null,sortOrder);
			MatrixCursor mx=new MatrixCursor(new String[]{DatabaseHelper.COLUMN_NAME_TITLE,DatabaseHelper.COLUMN_NAME_SUBTITLE});
			int i=0;
			int len=avd.size();
			while (i<len)
			{
			    String port=avdports.get(i);
			    // Forward * query to all avds
				if(!myPort.equals(port))
				{
				    String portToSend = avdports.get(i);
					String starForward="star"+"."+ portToSend;
					try {
						String str;
						str=new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, starForward,myPort).get();
						// Add key value pairs to cursor to query for each local avd
                        String no_data= "no data";
						if(!str.equals(no_data))
						{
							String[] split;
							split=str.split("\\$");
							Log.i("From client", split.toString());
							for (String st:split)
							{
								String[] split1;
								Log.e("IN star", "***");
								split1=st.split("\\&");
								Log.e("Star value",split1[0]+" Val:"+split1[1]);
								mx.addRow(new String[]{split1[0],split1[1]});
							}
						}


					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					} catch (NullPointerException npe)
					{
						Log.i("Null","DEAD AVD");
					}


				}
				i++;
			}
			MergeCursor mxc=new MergeCursor(new Cursor[]{mx,c});
			return mxc;
		}
		// ================ Selecting key from local AVD's ==================//
		else if(selection.charAt(selection_length)=='-')
		{
			Log.e("KEY", "HERE");
			String newSel="key = "+"\'"+selection.substring(0,selection.length()-1)+"\'";
			String sql="Select * from "+DatabaseHelper.TABLE_NAME+" WHERE "+DatabaseHelper.COLUMN_NAME_TITLE+"=\'"+selection.substring(0,selection.length()-1)+"\'";
			c=rdb.rawQuery(sql,selectionArgs);
			//c=sqb.query(rdb)
		}
		// ======================== Query for key ===========================//
		else {

				String key_id="";
				String[] split;

				try{
					key_id = genHash(selection);
					Log.d("Selection: hash", selection+" ## "+key_id);
				}catch(NoSuchAlgorithmException nsa)
				{
					nsa.getStackTrace();
				}


				String node_id= calculate_Node(key_id);
				c=null;
				String toForward = selection+"~"+node_id;
				try {

					String str;
					try {

						str = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toForward, myPort).get();
						Log.d("STRIN-WITHOUT", str);
					} catch (Exception e) {
						int newNodeindex = avdports.indexOf(node_id) + 2;
						int size = avdports.size();
						int possibleIndex = newNodeindex % size;
						String newNode =selection+"~"+ avdports.get(possibleIndex);

						Log.e("Str sent", newNode);
						str = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newNode, myPort).get();
						Log.d("STRIN", str);
					}


					MatrixCursor mx = new MatrixCursor(new String[]{DatabaseHelper.COLUMN_NAME_TITLE, DatabaseHelper.COLUMN_NAME_SUBTITLE});
					split = str.split("\\%");
					Log.d("SPLIT-KEy,Value", split[0] + "###" + split[1]);

					mx.addRow(new String[]{split[0], split[1]});
					//Log.e("Mcursor",mx.getString(0)+"??"+mx.getString(1));
					return mx;
				}catch (InterruptedException e){
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}



		}

		return c;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket=serverSockets[0];
			try{
				String str;
				int len;
				while (true)
				{
					Socket socket=serverSocket.accept();
					DataInputStream di=new DataInputStream(socket.getInputStream());
					str=di.readUTF();
                    // Checking delimiter and performing insert, query and recovery tasks
					Log.d("Recieved", str);

                    // From Query in client
					if(str.indexOf('#')>=0)
					{
                        String[] read=str.split("#");
                        Log.d("READ", read[0]+"------"+read[1]);
                        String port=read[1];
                        String _selection=read[0]+"-";
                        Cursor c;
                        String sql="Select * from "+DatabaseHelper.TABLE_NAME+" WHERE "+DatabaseHelper.COLUMN_NAME_TITLE+"=\""+_selection+"\"";
                        Log.d("SELECTION @\\",_selection);

                        c = query(mUri, null, _selection, null, null);

                        c.moveToFirst();

                        String _key=c.getString(c.getColumnIndex(DatabaseHelper.COLUMN_NAME_TITLE));
                        String _value= c.getString(c.getColumnIndex(DatabaseHelper.COLUMN_NAME_SUBTITLE));

                        String toForward;
                        toForward=_key+"%"+_value;

                        try {
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF(toForward);
                            dataOutputStream.flush();
                            c.close();
                        }catch (IOException io)
                        {
                            io.getStackTrace();
                        }
                        socket.close();
					}
					// For Insert from Client
					else if(str.indexOf(':')>=0)
					{

                        String[] read=str.split("\\:");
                        ContentValues ServerCV=new ContentValues();
                        String read_String= str;
                        String _key= read[0];
                        String _value= read[1];

                        Log.d("Key->",_key);
                        Log.d("Val->",_value);

                        ServerCV.put(DatabaseHelper.COLUMN_NAME_TITLE,_key);
                        ServerCV.put(DatabaseHelper.COLUMN_NAME_SUBTITLE,_value);

                        insert(mUri, ServerCV);

                        Log.e("Forward to Insert->",_key+"-===-"+_value);
                        try {
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF("From" + myPort);
                            dataOutputStream.flush();
                        }catch (IOException io)
                        {
                            io.getStackTrace();
                        }
                        socket.close();

					}
					// ===============Query star from query() =========================//
					else if(str.equals("star"))
					{

						Cursor c;
                        String sql= "Select * from "+DatabaseHelper.TABLE_NAME;
						c = query(mUri, null, "@", null, null);

						String key_value="";
						String toForward="";
						if(c.moveToFirst())
						{
							while (!c.isAfterLast())
							{
								String _key=c.getString(c.getColumnIndex(DatabaseHelper.COLUMN_NAME_TITLE));
								String _value= c.getString(c.getColumnIndex(DatabaseHelper.COLUMN_NAME_SUBTITLE));
								key_value=_key+"&"+_value;
								toForward=toForward+"$"+key_value;
								c.moveToNext();
                            }
						}
						Log.e("ToForward", toForward);
						int len1=toForward.length();
						if(len1>0)
						{
							String Forward=toForward.substring(1,len1);
							Log.e("FORWARD STAR", Forward);
							DataOutputStream dataOutputStream=new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF(Forward);
							dataOutputStream.flush();
						}
						else {
							DataOutputStream dataOutputStream=new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF("no data");
							dataOutputStream.flush();
						}

					}
					//================== Checking list of failed msg and adding it to a list upon recovery ================//
					else if(str.equals("AVD alive"))
					{
						DataOutputStream dataOutputStream=new DataOutputStream(socket.getOutputStream());
						// check if failedmsg is not empty
						if(failedavd.size()!=0)
						{
							StringBuilder sendFailed= new StringBuilder();
							int i=0;
							while(i<failedavd.size())
							{
								sendFailed.append("!").append(failedavd.get(i));
								i++;
							}
							failedavd.clear();
							dataOutputStream.writeUTF(sendFailed.toString());
							dataOutputStream.flush();
						}
						else {
							dataOutputStream.writeUTF("empty");
							dataOutputStream.flush();
						}
						socket.close();
					}



				}
			}
			catch (IOException io)
			{
				io.getStackTrace();
			}
			return null;
		}
	}
	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... strings) {



			try {
				//======== For insert==========//
				int char1= strings[0].indexOf(':');
				int char2= strings[0].indexOf('>');
				int char3= strings[0].indexOf('^');

				//======== For Query ==========//
				int char4= strings[0].indexOf('.');
				int char5= strings[0].indexOf('~');


                //======== For recovery =======//
				String alive=strings[0];

				// =========== Receiving from insert ================//
				if(char1>=0 && char2>=0)
				{

					String[] split;
					String my_Port;
					Log.d("STRINGS", strings[0]+"XXXXX"+strings[1]);
					split=strings[0].split("\\>");
					Log.d("SPLIT",split[0]+":"+split[1]);
					my_Port=split[1];

					ArrayList<String> currentPorts;
					Log.d("MYPORT", my_Port);


					currentPorts= setReplicas(avdports.indexOf(my_Port), (avdports.indexOf(my_Port)+1)%avdports.size(), (avdports.indexOf(my_Port)+2)%avdports.size());

					for(String port: currentPorts)
					{
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.valueOf(port));
						String msgTosend= split[0] + "<";

						DataOutputStream dataOutputStream= new DataOutputStream(socket.getOutputStream());
						dataOutputStream.writeUTF(msgTosend);
						dataOutputStream.flush();
						try {
							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
							String ack = dataInputStream.readUTF();
							Log.d(TAG, ack);
						}catch (Exception e)
						{
							Log.e("AVD is dead", port);
							failedPort=port;
							failedavd.add(msgTosend);
						}
						socket.close();
					}
				}
				//=========== Recieving from Insert, - In ring then insert to self and next 2 nodes ======================//
				else if(char1 >=0 && char3>=0)
				{
					ArrayList<String> currentPorts;

					currentPorts = setReplicas(0,1,2);

					for(String port: currentPorts)
					{
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.valueOf(port));
						Log.d("String to send", strings[0]);
						String msgTosend= strings[0];
						try {
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

							dataOutputStream.writeUTF(msgTosend);
							dataOutputStream.flush();
							try {
								DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
								String ack = dataInputStream.readUTF();
								Log.d(TAG, ack);
							} catch (Exception e) {
								Log.e("AVD is dead", port);
								failedPort = port;
								failedavd.add(msgTosend);
							}
						}catch (IOException io)
						{
							io.getStackTrace();
						}
						socket.close();
					}

				}

				//================= Sending query to AVD's ===============//
				else if(char5>=0)
				{
					String[] split;
					String my_Port,_key, msgToSend,read;

					split=strings[0].split("\\~");
					my_Port=split[1];
					_key=split[0];
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.valueOf(my_Port));
					msgToSend=_key+"#"+my_Port;
					DataOutputStream dataOutputStream=new DataOutputStream(socket.getOutputStream());
					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					dataOutputStream.writeUTF(msgToSend);
					dataOutputStream.flush();
					try {

						read = dataInputStream.readUTF();
						socket.close();
						return  read;
					}catch (Exception e)
					{
						socket.close();
						int index=avdports.indexOf(split[1])+2;
						int newIndex=index%avdports.size();
						my_Port=avdports.get(newIndex);
						socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.valueOf(my_Port));
						dataOutputStream=new DataOutputStream(socket.getOutputStream());
						dataOutputStream.writeUTF(msgToSend);

						dataOutputStream.flush();
						read=dataInputStream.readUTF();
						socket.close();
						return read;
					}


				}

				//================= From query * ==================//
				else if(char4>=0)
				{
					String[] split;
					String my_Port,msgToSend,read;

					split=strings[0].split("\\.");
					Log.d("STAR split", split[0]+"====="+split[1]);
					msgToSend=split[0];
					my_Port=split[1];
					Log.d(""+msgToSend,"\\To"+my_Port);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.valueOf(my_Port));
					DataOutputStream dataOutputStream=new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(msgToSend);
					dataOutputStream.flush();
					DataInputStream dataInputStream=new DataInputStream(socket.getInputStream());
					try {
						read = dataInputStream.readUTF();
						socket.close();
						return read;
					}catch (Exception e)
					{
						Log.e("THE AVD IS DEAD", my_Port);
						return null;
					}


				}
				else if(alive.equals("AVD alive"))
				{
					int i=0;
					while(i<REMOTE_PORT.length)
					{
						if(!REMOTE_PORT[i].equals(myPort))
						{}
						Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}), Integer.parseInt(REMOTE_PORT[i]));
						String msgToSend,str;
						msgToSend=strings[0];
						try {
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF(msgToSend);
							dataOutputStream.flush();
							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
							str = dataInputStream.readUTF();

							if (!str.equals("empty")) {
								String[] split;

								Log.e("STRREC", str);
								split = str.split("\\!");
								Log.e("Recovered rec", split[1] + "$$$$$$" + split[1]);
								for (String key_val : split) {
									if (key_val.equals("")) {
										continue;
									}
									String[] split_keyval;
									ContentValues cv = new ContentValues();
									Log.e("In aa", "RECOVERING");
									split_keyval = key_val.split("\\:");
									//Log.i("KEY_VAL", key_val);
									String _key = split_keyval[0];
									String _value = split_keyval[1];

									cv.put(DatabaseHelper.COLUMN_NAME_TITLE, _key);
									cv.put(DatabaseHelper.COLUMN_NAME_SUBTITLE, _value);

									insert(mUri, cv);

									cv.clear();
									socket.close();
								}
							} else {
								Log.e("NOthing here", "NAN");
							}


							i++;
						}catch (IOException io)
						{
							io.getStackTrace();
						}

					}


				}




			}catch(IOException io)
			{

				io.getStackTrace();
			}




			return null;
		}
	}


	// =================== Function to get the right port for the key and its replicas ==================//
	public String calculate_Node(String node)
	{
		for(int i=0; i<=avd.size()-2;i++)
		{
			int range= node.compareTo(avd.get(i));
			int nextRange= node.compareTo(avd.get(i+1));
			if(range>0 && nextRange<=0)
			{
				return avdports.get(i+1);
			}
		}
		return avdports.getFirst();
	}

	//================== Function to perform replication =======================//
	public ArrayList<String> setReplicas(int self, int first, int second)
	{
		ArrayList<String> currentPorts = new ArrayList<String>();
		String self_Replica = avdports.get(self);
		String first_Replica = avdports.get(first);
		String second_Replica = avdports.get(second);

		Log.d("Self", second_Replica);
		Log.e("First",first_Replica);
		Log.d("Second", second_Replica);

		currentPorts.add(self_Replica);
		currentPorts.add(first_Replica);
		currentPorts.add(second_Replica);

		return currentPorts;
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



