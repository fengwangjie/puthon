INSERT INTO `auth_role`(`id`, `org_id`, `name`) SELECT 1, `id`,'admin' FROM `core_org`;
INSERT INTO `auth_role`(`id`, `org_id`, `name`) SELECT 2, `id`,'teacher' FROM `core_org`;
INSERT INTO `auth_role`(`id`, `org_id`, `name`) SELECT 3, `id`,'student' FROM `core_org`;

INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       p.`id`,
       r.`org_id`
FROM `auth_role` r,
     `auth_permission` p
WHERE r.`name` = 'admin';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '102',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'student';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1101',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'student';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1102',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'student';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1103',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'student';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1104',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'student';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '211',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'student';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '101',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '102',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1101',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1102',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1103',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '1104',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '211',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '212',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';


INSERT INTO `auth_acl`(`role_id`, `permission_id`,`org_id`)
SELECT r.`id`,
       '213',
       r.`org_id`
FROM `auth_role` r
WHERE r.`name` = 'teacher';