-- Papéis que o Supabase cria automaticamente; recriados aqui para o warehouse local espelhar o remoto.
do $$ begin
  if not exists (select from pg_roles where rolname = 'anon') then create role anon nologin; end if;
  if not exists (select from pg_roles where rolname = 'authenticated') then create role authenticated nologin; end if;
end $$;
