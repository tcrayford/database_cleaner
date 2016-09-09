require 'database_cleaner/sequel/base'
require 'database_cleaner/generic/truncation'
require 'database_cleaner/sequel/truncation'

module DatabaseCleaner::Sequel
  class Deletion < Truncation
    def disable_referential_integrity(tables)
      case db.database_type
      when :postgres
        db.run('SET CONSTRAINTS ALL DEFERRED')
        trigger_disables = ""
        tables_to_truncate(db).each do |table|
          trigger_disables << "ALTER TABLE \"#{table}\" DISABLE TRIGGER ALL;"
        end
        db.run(trigger_disables)
      when :mysql
        old = db.fetch('SELECT @@FOREIGN_KEY_CHECKS').first[:@@FOREIGN_KEY_CHECKS]
        db.run('SET FOREIGN_KEY_CHECKS = 0')
      end
      yield
    ensure
      case db.database_type
      when :postgres
        tables.each do |table|
          db.run("ALTER TABLE \"#{table}\" ENABLE TRIGGER ALL")
        end
      when :mysql
        db.run("SET FOREIGN_KEY_CHECKS = #{old}")
      end
    end

    def delete_tables(db, tables)
      to_delete = ""
      tables.each do |table|
        to_delete << "DELETE FROM #{table};"
      end
      db.run(to_delete)
    end

    def clean
      return unless dirty?

      tables = tables_to_truncate(db)
      db.transaction do
        disable_referential_integrity(tables) do
          delete_tables(db, tables)
        end
      end
    end
  end
end
