local sqlite3 = require("lsqlite3")
local json= require( "LuaJSON_lib" )
require( "store_sql" )
function Transform( tbl, text )
    tbl.Words = {}
    tbl.WordPairs = {}
    tbl.HashTags = {}
    tbl.Refers = {}
    local lastword = nil
    local plaintext = nil
    for wrd in text:gmatch( "%S+" ) do
        local c = wrd:sub(1,1 )
        if c == "@" then
            tbl.Refers[ #tbl.Refers + 1] = wrd
        elseif c == "#" then
            tbl.HashTags[ #tbl.HashTags + 1] = wrd
        else
            tbl.Words[ #tbl.Words +1 ] = wrd
            if plaintext == nil then
               plaintext = wrd
            else
               plaintext = plaintext .. " " .. wrd
            end
            if lastword then
               tbl.WordPairs[ #tbl.WordPairs + 1 ] = lastword .. " " .. wrd
            end
            lastword = wrd
        end
    end
    plaintext = plaintext or ""
    tbl.Text = plaintext 
end

local db = sqlite3.open( arg[1] )
db:execute( "PRAGMA cache_size = 80000;" )
-- load schema
--dataFile = io.open( arg[2] , "r" )
--local schemaData = dataFile:read( "*a" )
--local schema = json.decode( schemaData )
--dataFile:close()
local schema = require( "schema" )

for i,j in ipairs( schema.Tables ) do
   db:execute( j )
end

for i,j in ipairs( schema.Indices ) do
   db:execute( j )
end

local dataFile =io.open (arg[2] ,"r" )
count = 0
db:execute( "BEGIN TRANSACTION;" );
lp = 0
for line in dataFile:lines() do
    local metaData = { ["$FileName"] = arg[2] }
    if line then 
        local data = nil
        if pcall( function() data = json.decode( line ) end ) then
            if data and data.user then 
                local transformed ={ tweetid = data.id, username = data.user.name }
                Transform( transformed, data.text )
                io.write( lp .. " " .. #transformed.HashTags .. " " .. #transformed.Refers .. " ".. #transformed.Words .."       \r")
                store_sql.StoreSQL( transformed, metaData, schema, db )
                count = count + 1
                lp = lp + 1 
                if count > 400 then
                    db:execute( "COMMIT;" );
                    db:execute( "BEGIN TRANSACTION;" );
                    count = 0
                end
            end    
        end
     end
end
db:execute( "COMMIT;" );

for i,j in ipairs( schema.Indices ) do
   db:execute( j )
end


print( "foo = ", foo )
require("serialize" )
