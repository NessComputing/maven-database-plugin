group sql;

drop_database(database) ::= <<
  DROP DATABASE <database>;
>>

create_database(database, owner, tablespace) ::= <<
  CREATE DATABASE <database> OWNER <owner>
                             ENCODING = 'utf8'
  <if(tablespace)> TABLESPACE = <tablespace><endif>;
>>

create_language() ::= <<
  CREATE LANGUAGE plpgsql;
>>

create_user(user, password) ::= <<
  CREATE ROLE <user> PASSWORD '<password>' LOGIN;
>>

detect_language() ::= <<
   SELECT count(*) FROM pg_language WHERE lanname='plpgsql';
>>

detect_user() ::= <<
   SELECT count(*) FROM pg_roles WHERE rolname=:user;
>>

detect_database() ::= <<
   SELECT count(*) FROM pg_database WHERE datname=:database;
>>
