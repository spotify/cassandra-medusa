Feature: Integration tests
    In order to run integration tests
    We'll spin up a Cassandra cluster

    Scenario Outline: Perform a backup, verify it, and restore it
        Given I have a fresh ccm cluster running named "scenario1"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        And the backup named "first_backup" has 2 SSTables for the "test" table in keyspace "medusa"
        And I can verify the backup named "first_backup" successfully
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        Given I have "300" rows in the "medusa.test" table
        When I restore the backup named "first_backup"
        Then I have "200" rows in the "medusa.test" table

        Examples:
        | Storage   |
        | local      |
#        | google_storage      |

    Scenario Outline: Perform a backup and verify its index
        Given I have a fresh ccm cluster running named "scenario2"
        And I am using "local" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup of the node named "second_backup"
        Then I can see the backup index entry for "second_backup"
        And I can see the latest backup for "localhost" being called "second_backup"
        Given I perform a backup of the node named "third_backup"
        Then I can see the backup index entry for "second_backup"
        Then I can see the backup index entry for "third_backup"
        And I can see the latest backup for "localhost" being called "third_backup"

        Examples:
        | Storage   |
        | local      |
#        | google_storage      |

    Scenario Outline: Perform a backup and verify the latest backup is updated correctly
        Given I have a fresh ccm cluster running named "scenario3"
        And I am using "<Storage>" as storage provider
        Then there is no latest backup for node "localhost"
        Given I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup of the node named "fourth_backup"
        And I can see the latest backup for "localhost" being called "fourth_backup"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup of the node named "fifth_backup"
        And I can see the latest backup for "localhost" being called "fifth_backup"

        Examples:
        | Storage   |
        | local      |
#        | google_storage      |        

    Scenario Outline: Perform a fake backup (by just writing an index) on different days and verify reports are correct
        Given I am using "<Storage>" as storage provider
        And node "n1" fakes a complete backup named "backup1" on "2019-04-15 12:12:00"
        And node "n2" fakes a complete backup named "backup1" on "2019-04-15 12:14:00"
        Then the latest cluster backup is "backup1"
        And there is no latest complete backup
        When I truncate the backup index
        And node "n1" fakes a complete backup named "backup1" on "2019-04-01 12:14:00"
        And node "n2" fakes a complete backup named "backup1" on "2019-04-01 12:16:00"
        And node "n1" fakes a complete backup named "backup2" on "2019-04-02 12:14:00"
        Then the latest cluster backup is "backup2"
        And there is no latest complete backup
        When I truncate the backup index
        And node "n1" fakes a complete backup named "backup1" on "2019-04-01 12:14:00"
        And node "n2" fakes a complete backup named "backup1" on "2019-04-01 12:16:00"
        And node "n1" fakes a complete backup named "backup2" on "2019-04-02 12:14:00"
        And node "n2" fakes a complete backup named "backup3" on "2019-04-03 12:14:00"
        Then the latest cluster backup is "backup3"
        And there is no latest complete backup
        When node "n2" fakes a complete backup named "backup2" on "2019-04-04 12:14:00"
        Then the latest cluster backup is "backup3"
        And there is no latest complete backup
        When node "n1" fakes a complete backup named "backup3" on "2019-04-05 12:14:00"
        Then the latest cluster backup is "backup3"
        And there is no latest complete backup
        When node "n3" fakes a complete backup named "backup2" on "2019-04-05 13:14:00"
        Then the latest cluster backup is "backup3"
        And the latest complete cluster backup is "backup2"
        Then node "n3" fakes a complete backup named "backup3" on "2019-04-05 14:14:00"
        Then the latest cluster backup is "backup3"
        And the latest complete cluster backup is "backup3"

        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test

    Scenario Outline: Verify re-creating index works
        Given I have a fresh ccm cluster running named "scenario4"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup of the node named "second_backup"
        And I perform a backup of the node named "third_backup"
        And I truncate the backup index
        Then there is no latest complete backup
        When I re-create the backup index
        Then the latest cluster backup is "third_backup"
        And the latest complete cluster backup is "third_backup"

        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test
