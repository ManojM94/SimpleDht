package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.UserDictionary;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.widget.TextView;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;
import static android.provider.UserDictionary.Words._ID;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    NavigableMap<String, String> treeMap = new TreeMap<String, String>();
    String selfHash;
    String prevNode = "null";
    String nextNode = "null";
    String prevNodeHash;
    String nextNodeHash;
    static int c=0;
    private ContentResolver mContentResolver;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");


    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledht.provider";
    static final String URL = "content://" + PROVIDER_NAME ;
    static final Uri CONTENT_URI = Uri.parse(URL);
    String myPort;
    /**
     * Database specific constant declarations
     */

    private SQLiteDatabase db;
    DatabaseHelper dbHelper;
    static final String DATABASE_NAME = "MSG";
    static final String MESSAGES_TABLE_NAME = "messages";
    static final int DATABASE_VERSION = 1;
    static final String CREATE_DB_TABLE =
            " CREATE TABLE " + MESSAGES_TABLE_NAME +
                    " (_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "[key] TEXT NOT NULL, " +
                    "value TEXT NOT NULL);";

    /**
     * Helper class that actually creates and manages
     * the provider's underlying data repository.
     */


    @Override
    public boolean onCreate() {
        Context context = getContext();
        mContentResolver = getContext().getContentResolver();

        dbHelper = new DatabaseHelper(context);
        db = dbHelper.getWritableDatabase();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            selfHash = genHash(portStr);
            prevNodeHash = selfHash;
            nextNodeHash = selfHash;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No such algo on create");
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");

        }
        String msg = "NewConnect";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

        return db!=null;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        SQLiteDatabase sqLiteDatabase = dbHelper.getWritableDatabase();
        String[] p = {"key", "value"};

        String valHash = "";
        try {
            valHash = genHash(selection);
            if(selection.contains(":") && !selection.contains("*")){
                valHash = genHash(selection.split(":")[0]);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int response = 0;
        if (prevNode.equals("null") && nextNode.equals("null")) {

            if (selection.equals("*") || selection.equals("@")) {
                response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);
            } else {
                String[] whereArgs = {selection};
                response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, "[key]=?", whereArgs);
            }
            Log.e(TAG, "Response to delete "+ Integer.toString(response));

        } else if (selection.equals("@")) {
            response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);
            Log.e(TAG, "Response to delete1 "+ Integer.toString(response));
        } else if (selection.equals("*")) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete:"+myPort+":"+selection, Integer.toString(Integer.parseInt(nextNode)*2));
            response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);
            Log.e(TAG, "Response to delete2 "+ Integer.toString(response));
        } else if (selection.charAt(0) == '*') {
            String[] split = selection.split(":");
            if(!split[1].equals(Integer.toString(Integer.parseInt(nextNode)*2))){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete:" + split[1] + ":" + split[0], Integer.toString(Integer.parseInt(nextNode)*2));
            }
            response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);
            Log.e(TAG, "Response to delete3 "+ Integer.toString(response));
        } else if (valHash.compareTo(prevNodeHash) >= 0 && valHash.compareTo(selfHash) < 0 || ((prevNodeHash.compareTo(selfHash) > 0 && nextNodeHash.compareTo(selfHash) > 0) && (valHash.compareTo(prevNodeHash) >= 0 || valHash.compareTo(selfHash) < 0))) {
            String[] whereArgs = {selection};
            response =  sqLiteDatabase.delete(MESSAGES_TABLE_NAME, "[key]=?", whereArgs);
            Log.e(TAG, "Response to delete4 "+ Integer.toString(response));
        } else {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete:"+myPort+":"+selection, Integer.toString(Integer.parseInt(nextNode)*2));
        }
        Log.e(TAG, "Returning response "+ Integer.toString(response));
        return response;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String valHash = "";

        try {
            valHash = genHash(values.getAsString("key"));
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No such algo for value");
        }
        if(selfHash.compareTo(valHash)>=0 && prevNodeHash.compareTo(valHash)<0){
            long rowID = db.insert(	MESSAGES_TABLE_NAME, "", values);

            if (rowID > 0) {
                Uri _uri = ContentUris.withAppendedId(CONTENT_URI, rowID);
                getContext().getContentResolver().notifyChange(_uri, null);
                Log.e(TAG, "insert to self " + values.toString());
                return _uri;
            }


        }else if(prevNode.equals("null") && nextNode.equals("null")){
                long rowID = db.insert(	MESSAGES_TABLE_NAME, "", values);

                if (rowID > 0) {
                    Uri _uri = ContentUris.withAppendedId(CONTENT_URI, rowID);
                    getContext().getContentResolver().notifyChange(_uri, null);
                    Log.e(TAG, "insert to self1 " + values.toString());
                    return _uri;
                }


            }
        else if (prevNodeHash.compareTo(selfHash) > 0 && nextNodeHash.compareTo(selfHash) > 0) {

            Log.d(TAG, "This is first node in ring");
            if (valHash.compareTo(prevNodeHash) >= 0 || valHash.compareTo(selfHash) < 0) {
                long rowID = db.insert(	MESSAGES_TABLE_NAME, "", values);

                if (rowID > 0) {
                    Uri _uri = ContentUris.withAppendedId(CONTENT_URI, rowID);
                    getContext().getContentResolver().notifyChange(_uri, null);
                    Log.e(TAG, "insert to self2 " + values.toString());
                    return _uri;
                }

            } else {

                Log.e(TAG, "Forwarding to " + nextNode +" "+ values.toString());
                JSONObject jsonObject = new JSONObject();
                try {
                    jsonObject.put("key", values.getAsString("key"));
                    jsonObject.put("value", values.getAsString("value"));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert:"+jsonObject.toString(), Integer.toString(Integer.parseInt(nextNode)*2));
            }
        } else {

            Log.e(TAG, "Forwarding to1 " + nextNode+" "+ values.toString());
            JSONObject jsonObject = new JSONObject();
            try {
                jsonObject.put("key", values.getAsString("key"));
                jsonObject.put("value", values.getAsString("value"));
            } catch (JSONException e) {
                e.printStackTrace();
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert:"+jsonObject.toString(), Integer.toString(Integer.parseInt(nextNode)*2));
        }

        return null;

    }



    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public MatrixCursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(MESSAGES_TABLE_NAME);
        String[] p = {"key", "value"};

        String valHash = "";
        try {
            valHash = genHash(selection);
            if(selection.contains(":") && !selection.contains("*")){
                valHash = genHash(selection.split(":")[0]);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (prevNode.equals("null") && nextNode.equals("null")) {

            if (selection.equals("@") || selection.equals("*")) {
                System.out.println("You're here");

                selection = null;

                Cursor c = qb.query(db,	p,	selection,
                        selectionArgs,null, null, sortOrder);
                MatrixCursor matrixCursor = new MatrixCursor(p);

                c.moveToFirst();
                while (!c.isAfterLast()) {
                    Object[] val = {c.getString(0), c.getString(1)};
                    matrixCursor.addRow(val);
                    c.moveToNext();
                }
                c.close();
               return matrixCursor;
            } else {
                selection = "\"" + selection +"\"";
                qb.appendWhere( "[key]" + "=" + selection);
                selection = null;


                Cursor c = qb.query(db,	projection,	selection,
                        selectionArgs,null, null, sortOrder);
                MatrixCursor matrixCursor = new MatrixCursor(p);
                c.moveToFirst();
                while (!c.isAfterLast()) {
                    Object[] val = {c.getString(1), c.getString(2)};
                    matrixCursor.addRow(val);
                    c.moveToNext();
                }
                c.close();
                return matrixCursor;

            }
        }else if (selection.equals("@")) {
            selection = null;

            Cursor c = qb.query(db,	p,	selection,
                    selectionArgs,null, null, sortOrder);

            MatrixCursor matrixCursor = new MatrixCursor(p);
            c.moveToFirst();
            while (!c.isAfterLast()) {
                Object[] val = {c.getString(0), c.getString(1)};
                matrixCursor.addRow(val);
                c.moveToNext();
            }
            c.close();
            return matrixCursor;
        } else if (selection.equals("*")) {
            MatrixCursor matrixCursor = new MatrixCursor(p);
           try {
                String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, Integer.toString(Integer.parseInt(nextNode)*2)).get();
               Log.e(TAG, "This is the response " + response);
               if (!response.equals("I got nothing")) {
                   String[] res = response.split(",");
                   Log.e(TAG, "This is the response " + response);


                   for (int i = 0; i < res.length; i++) {
                       matrixCursor.addRow(res[i].split(":"));
                   }
               }
               selection = null;
                Cursor c = qb.query(db,	p,	selection,
                        selectionArgs,null, null, sortOrder);
                c.moveToFirst();
                while (!c.isAfterLast()) {
                    Object[] val = {c.getString(0), c.getString(1)};
                    matrixCursor.addRow(val);
                    c.moveToNext();
                }
                return matrixCursor;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        } else if (selection.charAt(0) == '*') {
            MatrixCursor matrixCursor = new MatrixCursor(p);
            String[] split = selection.split(":");
            try {
            if (!split[1].equals(Integer.toString(Integer.parseInt(nextNode)*2))) {

                String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:" + split[1] + ":" + split[0], Integer.toString(Integer.parseInt(nextNode) * 2)).get();
                if (!response.equals("I got nothing")) {

                    String[] res = response.split(",");
                    Log.e(TAG, "This is the response " + response);


                    for (int i = 0; i < res.length; i++) {
                        matrixCursor.addRow(res[i].split(":"));
                    }
                }
            }
                    selection = null;
                    Cursor c = qb.query(db,	p,	selection,
                            selectionArgs,null, null, sortOrder);
                    c.moveToFirst();
                    while (!c.isAfterLast()) {
                        Object[] val = {c.getString(0), c.getString(1)};
                        matrixCursor.addRow(val);
                        c.moveToNext();
                    }
                    return matrixCursor;
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


         else if (valHash.compareTo(prevNodeHash) >= 0 && valHash.compareTo(selfHash) < 0 || ((prevNodeHash.compareTo(selfHash) > 0 && nextNodeHash.compareTo(selfHash) > 0) && (valHash.compareTo(prevNodeHash) >= 0 || valHash.compareTo(selfHash) < 0))) {
            Log.e(TAG, "I'm going to return value stored " + selection);
            selection = "\"" + selection.split(":")[0] +"\"";
            qb.appendWhere( "[key]" + "=" + selection);
            selection = null;

            Cursor c = qb.query(db,	projection,	selection,
                    selectionArgs,null, null, sortOrder);
            c.moveToFirst();
            Log.e(TAG, "Here it is " + c.getString(c.getColumnIndex("value")));
            MatrixCursor matrixCursor = new MatrixCursor(p);
            c.moveToFirst();
            while (!c.isAfterLast()) {
                String[] val = {c.getString(1), c.getString(2)};
                matrixCursor.addRow(val);
                c.moveToNext();
            }
            return matrixCursor;
        } else {
            Log.e(TAG, "I am " + myPort + " I was asked by tester " + selection + " "+ nextNode);
            try {
                String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, Integer.toString(Integer.parseInt(nextNode)*2)).get();
                String[] res = response.split(",");
                Log.e(TAG, "This is the response " + response);

                MatrixCursor matrixCursor = new MatrixCursor(p);
                for(int i=0; i<res.length; i++){
                    matrixCursor.addRow(res[i].split(":"));
                }
                return matrixCursor;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        return null;






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














    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (!Thread.interrupted()) {
                    Socket s = serverSocket.accept();

                    String i = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
                    if(i.contains("NewConnect")){
                        c+=1;
                        String port = Integer.toString(Integer.parseInt(i.split(":")[1])/2);
                        String hPort = genHash(port);
                        String next;
                        String prev;
                        treeMap.put(hPort, port);
                        for (TreeMap.Entry e : treeMap.entrySet()) {
                            String hp = (String)e.getKey();
                            String p = (String) e.getValue();
                            try {
                                next = (String) treeMap.higherEntry(hp).getValue();
                            }
                            catch (Exception ex){
                                next = treeMap.firstEntry().getValue();
                            }
                            try {
                                prev = (String) treeMap.lowerEntry(hp).getValue();
                            }
                            catch (Exception ex){
                                prev = treeMap.lastEntry().getValue();
                            }
                            if(c==1){
                                prev = null;
                                next = null;
                            }
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(p)*2);

                            PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
                            printWriter.println("Pointers:"+prev+":"+next);
                            printWriter.flush();
                        }
                        s.close();
                    }
                    else if(i.contains("Pointers")){
                        String[] pointers = i.split(":");
                        prevNode = pointers[1];
                        nextNode = pointers[2];
                        prevNodeHash = genHash(prevNode);
                        nextNodeHash = genHash(nextNode);
                        s.close();
                       /* if(myPort.equals(REMOTE_PORT0)&& c==2){
                            query(mUri, null, "*", null, null);
                        }*/
                    }
                    else if(i.contains("Insert")){
                        String msg = i.substring(9).replace("{", "").replace("}", "").replace("\"","");
                        String[] msgs = msg.split(",");
                        String value = msgs[0].substring(6);
                        String key = msgs[1].substring(4);
                        ContentValues mContentValues = new ContentValues();
                        mContentValues.put(KEY_FIELD, key);
                        mContentValues.put(VALUE_FIELD, value);
                        mContentResolver.insert(mUri, mContentValues);
                        s.close();

                    }
                    else if(i.contains("Query")){
                            String[] query = i.split(":");
                            String result = ",";
                            Log.e(TAG, "I am " + myPort + " I was asked " + query[2]+":"+query[1]);
                            MatrixCursor cursor = query(UserDictionary.Words.CONTENT_URI , null, query[2] +":"+ query[1], null, null);
                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String val = cursor.getString(0)+":"+cursor.getString(1);
                                result = result+val;
                                cursor.moveToNext();
                                result = result+",";
                            }
                            Log.e(TAG, "I'm writing back the cursor to asker " + result);
                            PrintWriter printWriter = new PrintWriter(s.getOutputStream());
                        if(result.equals(",")){
                            printWriter.println("I got nothing");
                        }
                        else {
                            printWriter.println(result.substring(1, result.length() - 1));
                        }
                            printWriter.flush();
                            s.close();
                        }
                    else if(i.contains("Delete")){
                            String[] query = i.split(":");
                            int res = delete(mUri, query[2]+":"+query[1], null);
                            PrintWriter printWriter = new PrintWriter(s.getOutputStream());
                            printWriter.println(res);
                            printWriter.flush();
                            s.close();
                    }

                }
            }
            catch (IOException e)
            {
                Log.e(TAG, "Can't accept connection");
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "No such algo");
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            try {
                String[] remotePort = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
                if(msgs[0].equals("NewConnect")){
                    System.out.println("Connecting to AVD0");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort[0]));

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
                    printWriter.println(msgs[0]+":"+msgs[1]);
                    printWriter.flush();

                }else{
                    System.out.println("Connecting to successor");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
                    printWriter.println(msgs[0]);
                    printWriter.flush();
                    String i = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
                    return i;

                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                return null;
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                return null;
            }
            return null;
        }
    }














    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context){
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_DB_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " +  MESSAGES_TABLE_NAME);
            onCreate(db);
        }

    }
}
