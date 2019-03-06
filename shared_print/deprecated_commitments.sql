create table shared_print_deprecated like shared_print_commitments;
alter table shared_print_deprecated add column deprecation_date timestamp default now() not null;
-- Enum values: C=Copy,  D=damaged, E=committed in error, L=lost, M=Missing from Print Holdings
alter table shared_print_deprecated add column deprecation_status enum('C','D','E','L','M') not null; 
