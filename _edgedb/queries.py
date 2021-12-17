#
# Copyright (c) 2019 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import edgedb
import random


INSERT_PREFIX = 'insert_test__'


def connect(ctx):
    return edgedb.connect()


def close(ctx, conn):
    conn.close()


def load_ids(ctx, conn):
    d = conn.query_single('''
        WITH
            U := User {id, r := random()},
            M := Movie {id, r := random()},
            P := Person {id, r := random()}
        SELECT (
            users := array_agg((SELECT U ORDER BY U.r LIMIT <int64>$lim).id),
            movies := array_agg((SELECT M ORDER BY M.r LIMIT <int64>$lim).id),
            people := array_agg((SELECT P ORDER BY P.r LIMIT <int64>$lim).id),
        );
    ''', lim=ctx.number_of_ids)

    return dict(
        get_user=list(d.users),
        get_movie=list(d.movies),
        get_person=list(d.people),
        # re-use user IDs for update tests
        update_movie=list(d.movies),
        # generate as many insert stubs as "concurrency" to
        # accommodate concurrent inserts
        insert_user=[INSERT_PREFIX] * ctx.concurrency,
    )


def get_user(conn, id):
    return conn.query_single_json('''
        SELECT User {
            id,
            name,
            image,
            latest_reviews := (
                WITH UserReviews := User.<author[IS Review]
                SELECT UserReviews {
                    id,
                    body,
                    rating,
                    movie: {
                        id,
                        image,
                        title,
                        avg_rating
                    }
                }
                ORDER BY .creation_time DESC
                LIMIT 10
            )
        }
        FILTER .id = <uuid>$id
    ''', id=id)


def get_movie(conn, id):
    return conn.query_single_json('''
        SELECT Movie {
            id,
            image,
            title,
            year,
            description,
            avg_rating,

            directors: {
                id,
                full_name,
                image,
            }
            ORDER BY Movie.directors@list_order EMPTY LAST
                THEN Movie.directors.last_name,

            cast: {
                id,
                full_name,
                image,
            }
            ORDER BY Movie.cast@list_order EMPTY LAST
                THEN Movie.cast.last_name,

            reviews := (
                SELECT Movie.<movie[IS Review] {
                    id,
                    body,
                    rating,
                    author: {
                        id,
                        name,
                        image,
                    }
                }
                ORDER BY .creation_time DESC
            ),
        }
        FILTER .id = <uuid>$id
    ''', id=id)


def get_person(conn, id):
    return conn.query_single_json('''
        SELECT Person {
            id,
            full_name,
            image,
            bio,

            acted_in := (
                WITH M := Person.<cast[IS Movie]
                SELECT M {
                    id,
                    image,
                    title,
                    year,
                    avg_rating
                }
                ORDER BY .year ASC THEN .title ASC
            ),

            directed := (
                WITH M := Person.<directors[IS Movie]
                SELECT M {
                    id,
                    image,
                    title,
                    year,
                    avg_rating
                }
                ORDER BY .year ASC THEN .title ASC
            ),
        }
        FILTER .id = <uuid>$id
    ''', id=id)


def update_movie(conn, id):
    return conn.query_single_json('''
        SELECT (
            UPDATE Movie
            FILTER .id = <uuid>$id
            SET {
                title := .title ++ '---' ++ <str>$suffix
            }
        ) {
            id,
            title
        }
    ''', id=id, suffix=str(id)[:8])


def insert_user(conn, val):
    num = random.randrange(1_000_000)
    return conn.query_single_json('''
        SELECT (
            INSERT User {
                name := <str>$name,
                image := <str>$image,
            }
        ) {
            id,
            name,
            image,
        }
    ''', name=f'{val}{num}', image=f'image_{val}{num}')


def setup(ctx, conn, queryname):
    if queryname == 'update_movie':
        conn.execute('''
            update Movie
            filter contains(.title, '---')
            set {
                title := str_split(.title, '---')[0]
            };
        ''')
    elif queryname == 'insert_user':
        conn.query('''
            delete User
            filter .name LIKE <str>$prefix
        ''', prefix=f'{INSERT_PREFIX}%')


def cleanup(ctx, conn, queryname):
    if queryname in {'update_movie', 'insert_user'}:
        # The clean up is the same as setup for mutation benchmarks
        setup(ctx, conn, queryname)
