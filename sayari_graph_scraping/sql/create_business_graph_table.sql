CREATE TABLE IF NOT EXISTS business_relationships (
    company_name_to TEXT NOT NULL,
    company_name_from TEXT NOT NULL,
    relationship_type VARCHAR(50) NOT NULL,
    UNIQUE (company_name_to, company_name_from, relationship_type)
)     