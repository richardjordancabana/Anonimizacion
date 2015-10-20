
drop database test;
CREATE DATABASE test;
use test;
CREATE TABLE tabla1 (DNI VARCHAR(15),Edad int,CP VARCHAR(10), Diagnostico VARCHAR(1000));
INSERT INTO tabla1 VALUES("50000",40,"28008","Tiene fiebre y vómitos");
INSERT INTO tabla1 VALUES("50001",4,"28010","Dolor de cabeza");
INSERT INTO tabla1 VALUES("50002",22,"28011","Fractura de  radio");
INSERT INTO tabla1 VALUES("50003",56,"28012","Fractura de cúbito");
INSERT INTO tabla1 VALUES("50004",56,"28012","Fractura de fémur");
INSERT INTO tabla1 VALUES("50005",6,"28010","Fractura de tibia");
INSERT INTO tabla1 VALUES("50006",1,"28013","Esguince de tobillo");
INSERT INTO tabla1 VALUES("50007",56,"28012","Fractura de cráneo");
INSERT INTO tabla1 VALUES("50008",6,"28010","Fractura de cúbito");
INSERT INTO tabla1 VALUES("50009",6,"28010","Esguince de codo");
INSERT INTO tabla1 VALUES("50010",6,"28010","Esguince de rodilla");
                    

delete from tabla1 where DNI="50006";
delete from tabla1 where DNI="50000";
delete from tabla1 where DNI="50001";
delete from tabla1 where DNI="50002";

 select min(cuenta)
 from (select count(*) as cuenta 
    from tabla1
    group by edad,CP) tabla
