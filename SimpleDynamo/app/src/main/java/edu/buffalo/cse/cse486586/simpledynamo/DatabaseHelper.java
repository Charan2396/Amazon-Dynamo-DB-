package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class DatabaseHelper extends SQLiteOpenHelper {

    public static final String COLUMN_NAME_TITLE = "key";
    public static final String COLUMN_NAME_SUBTITLE = "value";
    public static final String TABLE_NAME = "SimpleDynamo";

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + TABLE_NAME + "( " +
                    COLUMN_NAME_TITLE +" TEXT,"+
                    COLUMN_NAME_SUBTITLE+" TEXT, " +
                    " UNIQUE(" + COLUMN_NAME_TITLE + ") ON CONFLICT REPLACE);";

    public DatabaseHelper(Context context) {

        super(context, "Simpledynamo.db", null, 2);
    }

    @Override
    public void onCreate(SQLiteDatabase db)
    {
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
        onCreate(db);
    }
}
