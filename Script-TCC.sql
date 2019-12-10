
create schema "Staging Area";

create schema "Data Warehouse";

-----------------	Tabelas ExtraÃ§Ã£o

CREATE TABLE "Staging Area".registro_extracao(

	base_data timestamp,	
	base_cidade varchar(255),
	base_endereco varchar(255),
	base_branco varchar(255),
	base_placa varchar(255),
	base_modelo varchar(255),
	base_cor varchar(255),
	base_estado varchar(255),
	base_dezena varchar(255),
	base_unidade varchar(255),
	base_situacao varchar(255),
	base_ais varchar(255),
	base_cidadex varchar(255),
	base_tipovia varchar(255),		
	base_local_referencia varchar(255)		

);

CREATE TABLE "Staging Area".sensor_extracao(

	sensor_id int,
	sensor_id_2 int,
	sensor_endereco varchar(255),
	sensor_cidade varchar(255),
	sensor_tipovia varchar(255),
	sensor_local_referencia varchar(255),
	sensor_latitude float,
	sensor_longitude float

);
-------------------------------------------

--	1
---- CRIANDO TBELA "Staging Area".registro_trajetoria
CREATE TABLE "Staging Area".registro_trajetoria(
	id_registro serial NOT null primary key,
	id_trajetoria BIGSERIAL,
	base_data "timestamp",
	tipo_ponto varchar,
	
	base_cidade varchar(255),
	base_endereco varchar(255),
	base_placa varchar(255),
	base_modelo varchar(255),
	base_tipovia varchar(255),		
	base_local_referencia varchar(255),	

	sensor_id int,
	sensor_endereco varchar(255),
	sensor_cidade varchar(255),
	sensor_tipovia varchar(255),
	sensor_local_referencia varchar(255),
	sensor_latitude float,
	sensor_longitude float,
	pre_data "timestamp",
	pre_deltat interval,
	prox_data  "timestamp",
	prox_deltaT interval
);

-----------------------------------------------------------------------------------

 --	2 
 ----  MINHA FUNCAO PARA NÃƒO INSERIR REGISTROS INVALIDOS
CREATE OR REPLACE FUNCTION "Staging Area".limparRegisgtros()
RETURNS trigger AS $$
BEGIN 	
--	NÃƒO INSERINDO REGISTROS DUPLICADOS
	if new.pre_deltat::interval = '00:00:00'::interval or new.prox_deltat::interval = '00:00:00'::interval then
		return null;
	--	REGISTROS NÃƒO DUPLICADOS
		else
		--	DEFININDO O FIM DE UMA TRAJETORIA APÃ“S 20 min
			if new.pre_deltat::interval > '00:20:00'::interval then
				new.pre_data = null;
			end if;
			if new.prox_deltaT ::interval > '00:20:00'::interval then
				new.prox_data = null;
			end if;
		--	NÃƒO INSERINDO REGISTROS ÃšNICOS
				if 	new.pre_data isnull and new.prox_data isnull then	
					return null;
				end if;
		return new;
	end if;
END;
$$ LANGUAGE plpgsql;

--	3
----- TRIGGER QUE ATIVA MINHA FUNCAO add_idTrajetoria()
CREATE TRIGGER limparRegistros_trigger BEFORE insert ON "Staging Area".registro_trajetoria
FOR EACH row EXECUTE PROCEDURE "Staging Area".limparRegisgtros();
   
--	4
----  MINHA FUNCAO PARA SETAR O ID DAS TRAJETORIAS
CREATE OR REPLACE FUNCTION "Staging Area".add_idTrajetoria()
RETURNS trigger AS $$
DECLARE
	interator int;
BEGIN 
	SELECT incremental INTO interator from "Staging Area".temp_tb;
		
		if new.pre_data isnull and new.prox_data notnull then
			
		UPDATE "Staging Area".registro_trajetoria SET id_trajetoria = interator, tipo_ponto = 'INICIAL'
		WHERE id_registro = NEW.id_registro;
			return new;
		elsif new.pre_data notnull and new.prox_data notnull then

		UPDATE "Staging Area".registro_trajetoria SET id_trajetoria = interator, tipo_ponto = 'INTERMEDIARIO'
		WHERE id_registro = NEW.id_registro;
			return new;
		elsif new.pre_data notnull and new.prox_data isnull then
		
		UPDATE "Staging Area".registro_trajetoria SET id_trajetoria = interator, tipo_ponto = 'FINAL'
		WHERE id_registro = NEW.id_registro;
			update "Staging Area".temp_tb set incremental = interator + 1;
			return new;
		end if;
END;
$$ LANGUAGE plpgsql;

--	5
---- TRIGGER QUE ATIVA MINHA FUNCAO add_idTrajetoria()
create TRIGGER idTrajetoria_trigger after insert ON "Staging Area".registro_trajetoria 
FOR EACH row EXECUTE PROCEDURE "Staging Area".add_idTrajetoria();

--	6
----	CRIANDO TABELA AUXILIAR QUE FUNCIONARÃ� PARA ADIÃ‡ÃƒO DO id_trajetoria
	create table "Staging Area".temp_tb(
		incremental bigserial
	);
	insert into "Staging Area".temp_tb values(1);
	
-------------------------------------------------

--	7
---	INSERINDO DADOS NA TABELA "Staging Area".registro_trajetoria
insert into "Staging Area".registro_trajetoria(
	base_data, 
	base_cidade,
	base_endereco,
	base_placa,
	base_modelo,
	base_tipovia,		
	base_local_referencia,
	sensor_id,
	sensor_endereco,
	sensor_cidade,
	sensor_tipovia,
	sensor_local_referencia,
	sensor_latitude,
	sensor_longitude,
	pre_data,
	pre_deltat,
	prox_data,
	prox_deltaT
)SELECT distinct
	base_data,
	base_cidade,
	base_endereco,
	base_placa,
	base_modelo,
	base_tipovia,
	base_local_referencia,
	sensor_id, sensor_endereco, sensor_cidade,
	sensor_tipovia, sensor_local_referencia, sensor_latitude,
	sensor_longitude,
		LAG (b.base_data, 1) OVER (PARTITION BY b.base_placa ORDER by b.base_data)
	AS pre_data, b.base_data - LAG (b.base_data, 1) 
	OVER ( PARTITION BY b.base_placa ORDER by b.base_data)
	AS pre_deltat,
		LEAD(b.base_data, 1) OVER(PARTITION BY b.base_placa ORDER by b.base_data)
	AS prox_data, LEAD (b.base_data, 1) OVER ( PARTITION BY b.base_placa ORDER by b.base_data)  - b.base_data
    AS prox_deltaT
FROM "Staging Area".registro_extracao b inner join "Staging Area".sensor_extracao s on b.base_cidade = s.sensor_cidade and
b.base_endereco = s.sensor_endereco and b.base_local_referencia = s.sensor_local_referencia
WHERE s.sensor_cidade = 'FORTALEZA' AND b.base_data between '2017-09-06 00:00:00' and '2017-09-12 23:59:59'
order by base_placa, base_data;


-----------------------------------------------------------------------------------------------------

--	8
---- CRIANDO TABELA "Data Warehouse".dim_trajetoria

create table "Data Warehouse".dim_trajetoria(
	sk_trajetoria serial not null primary key,
	id_trajetoria int,
	duracao_trajetoria interval
);

--	9
---- POPULANDO TABELA "Data Warehouse".dim_trajetoria
insert into "Data Warehouse".dim_trajetoria(
	id_trajetoria,
	duracao_trajetoria
)SELECT INICIO.id_trajetoria , (FIM.time1 - INICIO.time2) duracao_trajetoria FROM
	(select id_trajetoria, base_data as time1 
	from "Staging Area".registro_trajetoria 
	where tipo_ponto = 'FINAL' 
	group by id_trajetoria, base_data 
	order by id_trajetoria) FIM,
	(select id_trajetoria, base_data as time2
	from "Staging Area".registro_trajetoria
	where tipo_ponto = 'INICIAL'
	group by id_trajetoria, base_data
	order by id_trajetoria) INICIO
WHERE FIM.id_trajetoria = INICIO.id_trajetoria;
---------------------------------------------------

--	10
-----	CRIANDO TABELA "Data Warehouse".dim_sensor	--------------------------------------
CREATE TABLE "Data Warehouse".dim_sensor ( 
	sk_sensor serial NOT NULL PRIMARY KEY,
	id_sensor int,
	endereco varchar(255),
	local_referencia varchar(255),
	latitude double precision,
	longitude double precision,
	cidade varchar(255),
	tipovia varchar(5)
);


--	11
-----	POPULANDO "Data Warehouse".dim_sensor	-------
INSERT INTO "Data Warehouse".dim_sensor ( 
	id_sensor,
	endereco,
	local_referencia,
	latitude,
	longitude,
	cidade,
	tipovia)
SELECT distinct b.sensor_id, b.sensor_endereco, b.sensor_local_referencia,
b.sensor_latitude, b.sensor_longitude, b.base_cidade, b.sensor_tipovia
from "Staging Area".registro_trajetoria b order by b.sensor_id;

---------------------------------------------------------------------------

--	12
---	CRIANDO TABELA "Data Warehouse".dim_veiculo	-------------------------------
CREATE TABLE "Data Warehouse".dim_veiculo(
	sk_veiculo serial NOT NULL PRIMARY KEY,
	placa varchar(255),
	modelo varchar(255)
);
-----------------------------

--	13
-----	POPULANDO TABELA "Data Warehouse".dim_veiculo	--------------

INSERT INTO "Data Warehouse".dim_veiculo(
	placa ,
	modelo )
SELECT distinct b.base_placa, b.base_modelo
FROM "Staging Area".registro_trajetoria b WHERE b.base_placa != '' 
order by b.base_placa;

------------------------------------------------------------------


--	14
---	CRIANDO TABELA "Data Warehouse".dim_tempo	-------------------------------
CREATE TABLE "Data Warehouse".dim_tempo(
	sk_tempo serial NOT NULL PRIMARY KEY,
	"data" TIMESTAMP,
	ano DOUBLE PRECISION,
	mes DOUBLE PRECISION,
	dia_semana VARCHAR(15),
	dia DOUBLE PRECISION,
	hora DOUBLE PRECISION
);
----------------------------------------

--	15
-----	POPULANDO TABELA "Data Warehouse".dim_tempo	--------------

INSERT INTO "Data Warehouse".dim_tempo(
	"data" ,
	ano ,
	mes ,
	dia_semana,
	dia,
	hora
)SELECT distinct b.base_data,
EXTRACT(YEAR FROM b.base_data) AS ANO,
EXTRACT(MONTH FROM b.base_data) AS MES, 
to_char(b.base_data, 'day') as DIA_SEMANA,
EXTRACT(DAY FROM b.base_data) AS DIA,
EXTRACT(HOUR FROM b.base_data) AS HORA
FROM "Staging Area".registro_trajetoria b WHERE b.base_placa != '' 
AND b.sensor_endereco = b.base_endereco
AND b.sensor_local_referencia = b.base_local_referencia
AND b.sensor_cidade = b.base_cidade  order by b.base_data;

----------------------------------------------------------------------------------

--	16
----	CRIANDO TABELA "Data Warehouse".fato	-----
CREATE TABLE "Data Warehouse".fato(
	id_fato serial NOT NULL PRIMARY KEY,
	sk_veiculo INT,
	sk_tempo INT,
	sk_sensor INT,
	sk_trajetoria INT,
	tipo_ponto varchar,
	prox_deltat TEXT,
	prox_ponto_local varchar,
	
	FOREIGN KEY (sk_veiculo) REFERENCES "Data Warehouse".dim_veiculo (sk_veiculo),
	FOREIGN KEY (sk_tempo) REFERENCES "Data Warehouse".dim_tempo (sk_tempo),
	FOREIGN KEY (sk_sensor) REFERENCES "Data Warehouse".dim_sensor (sk_sensor),
	FOREIGN KEY (sk_trajetoria) REFERENCES "Data Warehouse".dim_trajetoria (sk_trajetoria)
);
---------------------------------------------------------



--	17
----	POPULANDO TABELA "Data Warehouse".fato	-----
INSERT INTO "Data Warehouse".fato(
	sk_veiculo,
	sk_tempo,
	sk_sensor,
	sk_trajetoria,
	tipo_ponto,
	prox_deltat,
	prox_ponto_local
)SELECT distinct v.sk_veiculo, t.sk_tempo, s.sk_sensor, traj.sk_trajetoria,
a.tipo_ponto, case when a.prox_deltat > '00:20:00' then '00:00:00'
else a.prox_deltat end as prox_deltat,
case when a.prox_data notnull 
then b.base_endereco else 'Fim' end as prox_ponto 
from "Staging Area".registro_trajetoria a inner join "Staging Area".registro_trajetoria b 
on a.id_trajetoria = b.id_trajetoria, "Data Warehouse".dim_veiculo v, "Data Warehouse".dim_tempo t, "Data Warehouse".dim_sensor s,
"Data Warehouse".dim_trajetoria traj
where (a.prox_data = b.base_data or a.prox_data isnull)
and a.sensor_id = s.id_sensor 
AND a.base_data = t."data"
AND a.base_placa = v.placa
and a.id_trajetoria = traj.id_trajetoria;

------------------------------------------------------------------------------

--	18
-- CONSULTA MATRIZ ORIGEM DESTINO PARA O TABLEU
select f1.id_fato as id_fato, sinicio.id_sensor as id_sensor_inicio, sfim.id_sensor as id_sensor_fim,
count(distinct dtraj.id_trajetoria) as quantidade_registros
from "Data Warehouse".dim_sensor sinicio, "Data Warehouse".dim_sensor sfim, "Data Warehouse".dim_trajetoria dtraj,
"Data Warehouse".fato f1, "Data Warehouse".fato f2 where f1.sk_sensor = sinicio.sk_sensor and f1.tipo_ponto = 'INICIAL' 
and f2.sk_sensor = sfim.sk_sensor and f2.tipo_ponto = 'FINAL' 
and f1.sk_trajetoria = dtraj.sk_trajetoria
and f2.sk_trajetoria = dtraj.sk_trajetoria
group by f1.id_fato, sinicio.id_sensor, sfim.id_sensor order by f1.id_fato, sinicio.id_sensor, sfim.id_sensor;