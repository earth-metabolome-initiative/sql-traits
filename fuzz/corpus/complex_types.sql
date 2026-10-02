CREATE TABLE geo_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    point POINT,
    data JSONB NOT NULL,
    tags TEXT[] DEFAULT '{}',
    metadata HSTORE,
    tsv TSVECTOR
);
