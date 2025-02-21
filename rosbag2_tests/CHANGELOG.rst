^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package rosbag2_tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

0.30.0 (2024-11-26)
-------------------
* Add "--sort" CLI option to the "ros2 bag info" command (`#1804 <https://github.com/ros2/rosbag2/issues/1804>`_)
* Improve the reliability of rosbag2 tests (`#1796 <https://github.com/ros2/rosbag2/issues/1796>`_)
* Contributors: Alejandro Hernández Cordero, Chris Lalancette, Michael Orlov, Nicola Loi, Sanoronas

0.29.0 (2024-09-03)
-------------------
* Small cleanups to the rosbag2 tests. (`#1792 <https://github.com/ros2/rosbag2/issues/1792>`_)
* Add computation of size contribution to info verb (`#1726 <https://github.com/ros2/rosbag2/issues/1726>`_)
* Bugfix for wrong timestamps in ros2 bag info (`#1745 <https://github.com/ros2/rosbag2/issues/1745>`_)
* Fix for a false negative integration test with bag split in recorder (`#1743 <https://github.com/ros2/rosbag2/issues/1743>`_)
* Contributors: Chris Lalancette, Michael Orlov, Nicola Loi

0.28.0 (2024-06-17)
-------------------
* Propagate "custom_data" and "ros_distro" in to the metadata.yaml file during re-indexing (`#1700 <https://github.com/ros2/rosbag2/issues/1700>`_)
* Sweep cleanup in rosbag2 recorder CLI args verification code (`#1633 <https://github.com/ros2/rosbag2/issues/1633>`_)
* Fix for regression in `open_succeeds_twice` and `minimal_writer_example` tests (`#1667 <https://github.com/ros2/rosbag2/issues/1667>`_)
* Add optional  '--topics' CLI argument for 'ros2 bag record' (`#1632 <https://github.com/ros2/rosbag2/issues/1632>`_)
* Bugfix for writer not being able to open again after closing (`#1599 <https://github.com/ros2/rosbag2/issues/1599>`_)
* Contributors: Cole Tucker, Michael Orlov, yschulz

0.27.0 (2024-04-30)
-------------------

0.26.1 (2024-04-17)
-------------------

0.26.0 (2024-04-16)
-------------------
* Use middleware send and receive timestamps from message_info during recording (`#1531 <https://github.com/ros2/rosbag2/issues/1531>`_)
* Added exclude-topic-types to record (`#1582 <https://github.com/ros2/rosbag2/issues/1582>`_)
* Contributors: Alejandro Hernández Cordero, jmachowinski

0.25.0 (2024-03-27)
-------------------
* Use std::filesystem instead of rcpputils::fs (`#1576 <https://github.com/ros2/rosbag2/issues/1576>`_)
* Filter topic by type  (`#1577 <https://github.com/ros2/rosbag2/issues/1577>`_)
* Make some changes for newer versions of uncrustify. (`#1578 <https://github.com/ros2/rosbag2/issues/1578>`_)
* Add topic_id returned by storage to the TopicMetadata (`#1538 <https://github.com/ros2/rosbag2/issues/1538>`_)
* Improve performance in SqliteStorage::get_bagfile_size() (`#1516 <https://github.com/ros2/rosbag2/issues/1516>`_)
* Implement service recording and display info about recorded services (`#1480 <https://github.com/ros2/rosbag2/issues/1480>`_)
* Mark play_end_to_end test as xfail in Windows (`#1452 <https://github.com/ros2/rosbag2/issues/1452>`_)
* Contributors: Alejandro Hernández Cordero, Barry Xu, Chris Lalancette, Cristóbal Arroyo, Michael Orlov, Roman Sokolkov

0.24.0 (2023-07-11)
-------------------
* Implement storing and loading ROS_DISTRO from metadata.yaml and mcap files (`#1241 <https://github.com/ros2/rosbag2/issues/1241>`_)
* Address flakiness in rosbag2_play_end_to_end tests (`#1297 <https://github.com/ros2/rosbag2/issues/1297>`_)
* Contributors: Emerson Knapp, Michael Orlov

0.23.0 (2023-04-28)
-------------------

0.22.0 (2023-04-18)
-------------------
* Add type_hash in MessageDefinition struct (`#1296 <https://github.com/ros2/rosbag2/issues/1296>`_)
* Contributors: Michael Orlov

0.21.0 (2023-04-12)
-------------------
* rosbag2_cpp: move local message definition source out of MCAP plugin (`#1265 <https://github.com/ros2/rosbag2/issues/1265>`_)
* Update rosbag2 to C++17. (`#1238 <https://github.com/ros2/rosbag2/issues/1238>`_)
* Use target_link_libraries instead of ament_target_dependencies (`#1202 <https://github.com/ros2/rosbag2/issues/1202>`_)
* Contributors: Chris Lalancette, Daisuke Nishimatsu, Michael Orlov, james-rms

0.20.0 (2023-02-14)
-------------------

0.19.0 (2023-01-13)
-------------------
* rosbag2_storage: set MCAP as default plugin (`#1160 <https://github.com/ros2/rosbag2/issues/1160>`_)
* Add Michael Orlov as maintainer in rosbag2 packages (`#1215 <https://github.com/ros2/rosbag2/issues/1215>`_)
* Parametrize all rosbag2_tests for both supported storage plugins (`#1221 <https://github.com/ros2/rosbag2/issues/1221>`_)
* Make rosbag2_tests agnostic to storage implementation (`#1192 <https://github.com/ros2/rosbag2/issues/1192>`_)
* Contributors: Emerson Knapp, Michael Orlov, james-rms

0.18.0 (2022-11-15)
-------------------
* Get rid from attempt to open DB file in `wait_for_db()` test fixture (`#1141 <https://github.com/ros2/rosbag2/issues/1141>`_)
* Fix for ros2 bag play exit with non-zero code on SIGINT (`#1126 <https://github.com/ros2/rosbag2/issues/1126>`_)
* Move sqlite3 storage implementation to rosbag2_storage_sqlite3 package (`#1113 <https://github.com/ros2/rosbag2/issues/1113>`_)
* Readers/info can accept a single bag storage file, and detect its storage id automatically (`#1072 <https://github.com/ros2/rosbag2/issues/1072>`_)
* Contributors: Emerson Knapp, Michael Orlov

0.17.0 (2022-07-30)
-------------------
* Add the ability to record any key/value pair in 'custom' field in metadata.yaml (`#1038 <https://github.com/ros2/rosbag2/issues/1038>`_)
* Contributors: Hunter L. Allen, Tony Peng

0.16.0 (2022-05-11)
-------------------

0.15.1 (2022-04-06)
-------------------
* Revert "Add the ability to record any key/value pair in the 'custom' field in metadata.yaml (`#976 <https://github.com/ros2/rosbag2/issues/976>`_)" (`#984 <https://github.com/ros2/rosbag2/issues/984>`_)
* Add the ability to record any key/value pair in the 'custom' field in metadata.yaml (`#976 <https://github.com/ros2/rosbag2/issues/976>`_)
* Contributors: Audrow Nash, Jorge Perez, Tony Peng

0.15.0 (2022-04-05)
-------------------
* Revert "Add the ability to record any key/value pair in the 'custom' field in metadata.yaml (`#976 <https://github.com/ros2/rosbag2/issues/976>`_)" (`#984 <https://github.com/ros2/rosbag2/issues/984>`_)
* Add the ability to record any key/value pair in the 'custom' field in metadata.yaml (`#976 <https://github.com/ros2/rosbag2/issues/976>`_)
* Contributors: Jorge Perez, Tony Peng

0.14.1 (2022-03-29)
-------------------
* Bump version number to avoid conflict
* Contributors: Chris Lalancette

0.14.0 (2022-03-29)
-------------------

0.13.0 (2022-01-13)
-------------------

0.12.0 (2021-12-17)
-------------------
* Add pause/resume options to the bag recorder (`#905 <https://github.com/ros2/rosbag2/issues/905>`_)
* Contributors: Ivan Santiago Paunovic

0.11.0 (2021-11-08)
-------------------
* Update package maintainers (`#899 <https://github.com/ros2/rosbag2/issues/899>`_)
* Contributors: Michel Hidalgo

0.10.1 (2021-10-22)
-------------------

0.10.0 (2021-10-19)
-------------------
* Fix a bug on invalid pointer address when using "MESSAGE" compressio… (`#866 <https://github.com/ros2/rosbag2/issues/866>`_)
* Metadata per file info (`#870 <https://github.com/ros2/rosbag2/issues/870>`_)
* Fix record test to reflect plugin query changes (`#838 <https://github.com/ros2/rosbag2/issues/838>`_)
* Make sure the subscription exists before publishing messages (`#804 <https://github.com/ros2/rosbag2/issues/804>`_)
* Handle SIGTERM gracefully in recording (`#792 <https://github.com/ros2/rosbag2/issues/792>`_)
* Add spin_and_wait_for_matched to PublicationManager and update test c… (`#797 <https://github.com/ros2/rosbag2/issues/797>`_)
* Remove rmw_fastrtps_cpp find_package in rosbag2_tests (`#774 <https://github.com/ros2/rosbag2/issues/774>`_)
* Contributors: Barry Xu, Cameron Miller, Emerson Knapp, Ivan Santiago Paunovic, Wojciech Jaworski

0.9.0 (2021-05-17)
------------------
* Correct expectation for exit code in play_end_to_end test since after redesign we are getting exception in constructor. (`#763 <https://github.com/ros2/rosbag2/issues/763>`_)
* remodel publication manager (`#749 <https://github.com/ros2/rosbag2/issues/749>`_)
* correct exit code assertion (`#747 <https://github.com/ros2/rosbag2/issues/747>`_)
* Contributors: Karsten Knese, Michael Orlov

0.8.0 (2021-04-19)
------------------
* Remove -Werror from builds, enable it in Action CI (`#722 <https://github.com/ros2/rosbag2/issues/722>`_)
* Explicitly add emersonknapp as maintainer (`#692 <https://github.com/ros2/rosbag2/issues/692>`_)
* Reindexer core (`#641 <https://github.com/ros2/rosbag2/issues/641>`_)
  Add a new C++ Reindexer class for reconstructing metadata from bags that are missing it.
* use rclcpp serialized messages to write data (`#457 <https://github.com/ros2/rosbag2/issues/457>`_)
* Contributors: Emerson Knapp, Karsten Knese, jhdcs

0.7.0 (2021-03-18)
------------------
* Alternative write api (`#676 <https://github.com/ros2/rosbag2/issues/676>`_)
* RMW-implementation-searcher converter in rosbag2_cpp (`#670 <https://github.com/ros2/rosbag2/issues/670>`_)
* Use rosbag2_py for ros2 bag info (`#673 <https://github.com/ros2/rosbag2/issues/673>`_)
* Remove temporary directory platform-specific logic from test fixture (`#660 <https://github.com/ros2/rosbag2/issues/660>`_)
* Fix --topics flag for ros2 bag play being ignored for all bags after the first one. (`#619 <https://github.com/ros2/rosbag2/issues/619>`_)
* Move zstd compressor to its own package (`#636 <https://github.com/ros2/rosbag2/issues/636>`_)
* Contributors: Alexander, Emerson Knapp, Karsten Knese

0.6.0 (2021-02-01)
------------------
* Fix relative metadata paths in SequentialCompressionWriter (`#613 <https://github.com/ros2/rosbag2/issues/613>`_)
* Recorder --regex and --exclude options (`#604 <https://github.com/ros2/rosbag2/issues/604>`_)
* Fix the tests on cyclonedds by translating qos duration values (`#606 <https://github.com/ros2/rosbag2/issues/606>`_)
* Contributors: Adam Dąbrowski, Emerson Knapp

0.5.0 (2020-12-02)
------------------

0.4.0 (2020-11-19)
------------------
* add storage_config_uri (`#493 <https://github.com/ros2/rosbag2/issues/493>`_)
* Removed duplicated code in record (`#534 <https://github.com/ros2/rosbag2/issues/534>`_)
* Change default cache size for sequential_writer to a non zero value (`#533 <https://github.com/ros2/rosbag2/issues/533>`_)
* Update the package.xml files with the latest Open Robotics maintainers (`#535 <https://github.com/ros2/rosbag2/issues/535>`_)
* Mark flaky tests as xfail for now (`#520 <https://github.com/ros2/rosbag2/issues/520>`_)
* introduce defaults for the C++ API (`#452 <https://github.com/ros2/rosbag2/issues/452>`_)
* Adding db directory creation to rosbag2_cpp (`#450 <https://github.com/ros2/rosbag2/issues/450>`_)
* minimal c++ API test (`#451 <https://github.com/ros2/rosbag2/issues/451>`_)
* Add split by time to recording (`#409 <https://github.com/ros2/rosbag2/issues/409>`_)
* Contributors: Emerson Knapp, Jaison Titus, Karsten Knese, Marwan Taher, Michael Jeronimo, jhdcs

0.3.2 (2020-06-03)
------------------

0.3.1 (2020-06-01)
------------------

0.3.0 (2020-05-26)
------------------
* Export targets (`#403 <https://github.com/ros2/rosbag2/issues/403>`_)
* Contributors: Karsten Knese

0.2.8 (2020-05-18)
------------------
* Disable play_filters_by_topic test (`#410 <https://github.com/ros2/rosbag2/issues/410>`_)
* Contributors: Mabel Zhang

0.2.7 (2020-05-12)
------------------
* Fix splitting tests on windows (`#407 <https://github.com/ros2/rosbag2/issues/407>`_)
* Fix `#381 <https://github.com/ros2/rosbag2/issues/381>`_ unstable play_end_to_end test (`#396 <https://github.com/ros2/rosbag2/issues/396>`_)
* Contributors: Karsten Knese, Mabel Zhang

0.2.6 (2020-05-07)
------------------
* Correct usage of rcpputils::SharedLibrary loading. (`#400 <https://github.com/ros2/rosbag2/issues/400>`_)
* Contributors: Karsten Knese

0.2.5 (2020-04-30)
------------------
* Expose topic filter to command line (addresses `#342 <https://github.com/ros2/rosbag2/issues/342>`_) (`#363 <https://github.com/ros2/rosbag2/issues/363>`_)
* Fix rosbag2_tests resource files and play_end_to_end test (`#362 <https://github.com/ros2/rosbag2/issues/362>`_)
* Replace poco dependency by rcutils (`#322 <https://github.com/ros2/rosbag2/issues/322>`_)
* resolve relative file paths (`#345 <https://github.com/ros2/rosbag2/issues/345>`_)
* Transaction based sqlite3 inserts (`#225 <https://github.com/ros2/rosbag2/issues/225>`_)
* Replace rcutils_get_file_size with rcpputils::fs::file_size (`#291 <https://github.com/ros2/rosbag2/issues/291>`_)
* [compression] Enable compression through ros2bag cli (`#263 <https://github.com/ros2/rosbag2/issues/263>`_)
* Wait for metadata to be written to disk (`#283 <https://github.com/ros2/rosbag2/issues/283>`_)
* Refactor record_fixture to use rcpputils::fs::path (`#286 <https://github.com/ros2/rosbag2/issues/286>`_)
* code style only: wrap after open parenthesis if not in one line (`#280 <https://github.com/ros2/rosbag2/issues/280>`_)
* Enhance E2E tests in Windows (`#278 <https://github.com/ros2/rosbag2/issues/278>`_)
* Add splitting e2e tests (`#247 <https://github.com/ros2/rosbag2/issues/247>`_)
* remove rosbag2 filesystem helper (`#249 <https://github.com/ros2/rosbag2/issues/249>`_)
* Make rosbag2 a metapackage (`#241 <https://github.com/ros2/rosbag2/issues/241>`_)
* [Compression - 7] Add compression metadata (`#221 <https://github.com/ros2/rosbag2/issues/221>`_)
* make ros tooling working group maintainer (`#211 <https://github.com/ros2/rosbag2/issues/211>`_)
* Contributors: Alejandro Hernández Cordero, Anas Abou Allaban, Dirk Thomas, Karsten Knese, Mabel Zhang, Sriram Raghunathan, Zachary Michaels

0.2.4 (2019-11-18)
------------------

0.2.3 (2019-11-18)
------------------
* Enhance rosbag writer capabilities to split bag files. (`#185 <https://github.com/ros2/rosbag2/issues/185>`_)
* Contributors: Zachary Michaels

0.2.2 (2019-11-13)
------------------
* (API) Generate bagfile metadata in Writer (`#184 <https://github.com/ros2/rosbag2/issues/184>`_)
* Contributors: Zachary Michaels

0.2.1 (2019-10-23)
------------------

0.2.0 (2019-09-26)
------------------
* disable plugins/tests which need rmw_fastrtps_cpp if unavailable (`#137 <https://github.com/ros2/rosbag2/issues/137>`_)
* Contributors: ivanpauno

0.1.2 (2019-05-20)
------------------

0.1.1 (2019-05-09)
------------------

0.1.0 (2019-05-08)
------------------
* fix compilation against master (`#111 <https://github.com/ros2/rosbag2/issues/111>`_)
* use fastrtps static instead of dynamic (`#104 <https://github.com/ros2/rosbag2/issues/104>`_)
* Compile tests (`#103 <https://github.com/ros2/rosbag2/issues/103>`_)
* remove duplicate repos (`#102 <https://github.com/ros2/rosbag2/issues/102>`_)
* removed dependency to ros1_bridge package (`#90 <https://github.com/ros2/rosbag2/issues/90>`_)
* Contributors: DensoADAS, Dirk Thomas, Karsten Knese

0.0.5 (2018-12-27)
------------------

0.0.4 (2018-12-19)
------------------
* 0.0.3
* Play old bagfiles (`#69 <https://github.com/bsinno/rosbag2/issues/69>`_)
* Contributors: Karsten Knese, Martin Idel

0.0.2 (2018-12-12)
------------------
* do not ignore return values
* update maintainer email
* Contributors: Karsten Knese, root

0.0.1 (2018-12-11)
------------------
* Auto discovery of new topics (`#63 <https://github.com/ros2/rosbag2/issues/63>`_)
* Split converters (`#70 <https://github.com/ros2/rosbag2/issues/70>`_)
* Fix master build and small renamings (`#67 <https://github.com/ros2/rosbag2/issues/67>`_)
* rename topic_with_types to topic_metadata
* iterate_over_formatter
* GH-142 replace map with unordered map where possible (`#65 <https://github.com/ros2/rosbag2/issues/65>`_)
* Use converters when recording a bag file (`#57 <https://github.com/ros2/rosbag2/issues/57>`_)
* Display bag summary using `ros2 bag info` (`#45 <https://github.com/ros2/rosbag2/issues/45>`_)
* Use directory as bagfile and add additonal record options (`#43 <https://github.com/ros2/rosbag2/issues/43>`_)
* Introduce rosbag2_transport layer and CLI (`#38 <https://github.com/ros2/rosbag2/issues/38>`_)
* Contributors: Alessandro Bottero, Andreas Greimel, Andreas Holzner, Karsten Knese, Martin Idel
