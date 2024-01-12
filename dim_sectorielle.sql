CREATE TABLE public.dim_sectorielle (
    id_secteur character varying(10) NOT NULL ENCODE bytedict,
    name_secteur character varying(255) ENCODE lzo,
    PRIMARY KEY (id_secteur)
) DISTSTYLE EVEN;
INSERT INTO dim_sectorielle (id_secteur, name_secteur) VALUES 
('OOC', 'Official Core Consumer'),
('EC', 'Energy Consumer'),
('FC', 'Food Consumer'),
('P', 'Producer'),
('G', 'GLOBAL');