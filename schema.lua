return 
{ 
    Tables = {
       "CREATE TABLE IF NOT EXISTS TwitterMain      ( ID INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, tweetid TEXT );",
       "CREATE TABLE IF NOT EXISTS TwitterWords     ( ID INTEGER, word TEXT );",
       "CREATE TABLE IF NOT EXISTS TwitterWordPairs ( ID INTEGER, word TEXT );",
       "CREATE TABLE IF NOT EXISTS TwitterHashTags  ( ID INTEGER, word TEXT );",
       "CREATE TABLE IF NOT EXISTS TwitterRefers    ( ID INTEGER, word TEXT );",
       "CREATE TABLE IF NOT EXISTS TwitterText      ( ID INTEGER, data TEXT );",
       

       "CREATE TABLE IF NOT EXISTS TwitterWords_Size     ( ID INTEGER, count INTEGER);",
       "CREATE TABLE IF NOT EXISTS TwitterWordPairs_Size     ( ID INTEGER, count INTEGER);",
       "CREATE TABLE IF NOT EXISTS TwitterHashTags_Size    ( ID INTEGER, count INTEGER );",
       "CREATE TABLE IF NOT EXISTS TwitterRefers_Size    ( ID INTEGER, count INTEGER );",

    },
    Indices = {
       "CREATE INDEX IF NOT EXISTS IdxTwitterWords_id_text ON TwitterWords( ID, word );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterWordPairss_id_text ON TwitterWordPairs( ID, word );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterHashTags_id_text ON TwitterHashTags( ID, word );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterRefers_id_text ON TwitterRefers( ID, word );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterText_id  ON   TwitterText ( ID );",
      
       "CREATE INDEX IF NOT EXISTS IdxTwitterWords_Size    ON TwitterWords_Size( ID );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterWordPairs_Size    ON TwitterWordPairs_Size( ID );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterHashTags_Size ON TwitterHashTags_Size( ID );",
       "CREATE INDEX IF NOT EXISTS IdxTwitterRefers_Size   ON TwitterRefers_Size( ID );",
    },
    DropIndices = {
       "DROP INDEX IF EXISTS IdxTwitterWords_id_text ON TwitterWords( ID, word );",
       "DROP INDEX IF EXISTS IdxTwitterWordPairss_id_text ON TwitterWordPairs( ID, word );",
       "DROP INDEX IF EXISTS IdxTwitterHashTags_id_text ON TwitterHashTags( ID, word );",
       "DROP INDEX IF EXISTS IdxTwitterRefers_id_text ON TwitterRefers( ID, word );",
      
       "DROP INDEX IF EXISTS IdxTwitterWords_Size    ON TwitterWords_Size( ID );",
       "DROP INDEX IF EXISTS IdxTwitterWordPairs_Size    ON TwitterWordPairs_Size( ID );",
       "DROP INDEX IF EXISTS IdxTwitterHashTags_Size ON TwitterHashTags_Size( ID );",
       "DROP INDEX IF EXISTS IdxTwitterRefers_Size   ON TwitterRefers_Size( ID );",
    },
    Order = {
       "Identity", 
       "Words", 
       "Text", 
       "WordPairs", 
       "HashTags",
       "Refers",
    },
    Identity = {
        Table = "TwitterMain",
        Type  = "Main",
        SQLInsert   = "INSERT INTO TwitterMain (username, tweetid ) VALUES ( ?,? );",
        Fields = {
            "username",
            "tweetid",
        },
    },
    Text = {
        Table = "TwitterText",
        SQLInsert       = "INSERT INTO TwitterText ( ID,  data) VALUES ( ?,? );",
        DoSize = true,
        Type  = "Table",
        Fields = {
            "$IDX",
            "Text",
         },
    },

    Words = {
        Table = "TwitterWords",
        SQLInsert       = "INSERT INTO TwitterWords ( ID, word) VALUES ( ?,? );",
        DoSize = true,
        Type  = "Array",
        Part  = "Words",
        Fields = {
            "$IDX",
            "$VALUE",
         },
    },
    WordPairs = {
        Table = "TwitterWordPairs",
        SQLInsert       = "INSERT INTO TwitterWordPairs ( ID, word) VALUES ( ?,? );",
        DoSize = true,
        Type  = "Array",
        Part  = "WordPairs",
        Fields = {
            "$IDX",
            "$VALUE",
         },
    },
    HashTags = {
        Table = "TwitterHashTags",
        SQLInsert       = "INSERT INTO TwitterHashTags ( ID, word) VALUES ( ?,? );",
        DoSize = true,
        Type  = "Array",
        Part  = "HashTags",
        Fields = {
            "$IDX",
            "$VALUE",
         },
    },
    Refers = {
        Table = "TwitterRefers",
        SQLInsert       = "INSERT INTO TwitterRefers ( ID, word) VALUES ( ?,? );",
        DoSize = true,
        Type  = "Array",
        Part  = "Refers",
        Fields = {
            "$IDX",
            "$VALUE",
         },
    },    
}
