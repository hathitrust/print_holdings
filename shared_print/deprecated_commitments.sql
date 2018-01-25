create table shared_print_deprecated like shared_print_commitments;
alter table shared_print_deprecated add column deprecation_date timestamp default now() not null;
-- Enum values: L=lost, D=damaged, E=committed in error
alter table shared_print_deprecated add column deprecation_status enum('L', 'D', 'E') not null; 
