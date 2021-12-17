#
# Copyright (c) 2019 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from django.db import connection
import json
import random

from . import bootstrap  # NoQA

from . import models
from . import views


INSERT_PREFIX = 'insert_test__'


def init(ctx):
    from django.conf import settings
    settings.DATABASES["default"]["HOST"] = ctx.db_host


def connect(ctx):
    # Django fully abstracts away connection management, so we
    # rely on it to create a new connection for every benchmark
    # thread.
    return None


def close(ctx, db):
    return


def load_ids(ctx, db):
    users = models.User.objects.raw('''
        SELECT * FROM _django_user ORDER BY random() LIMIT %s
    ''', [ctx.number_of_ids])

    movies = models.Movie.objects.raw('''
        SELECT * FROM _django_movie ORDER BY random() LIMIT %s
    ''', [ctx.number_of_ids])

    people = models.Person.objects.raw('''
        SELECT * FROM _django_person ORDER BY random() LIMIT %s
    ''', [ctx.number_of_ids])

    return dict(
        get_user=[d.id for d in users],
        get_movie=[d.id for d in movies],
        get_person=[d.id for d in people],
        # re-use user IDs for update tests
        update_movie=[d.id for d in movies],
        # generate as many insert stubs as "concurrency" to
        # accommodate concurrent inserts
        insert_user=[INSERT_PREFIX] * ctx.concurrency,
    )


def get_user(conn, id):
    record = models.User.objects.get(pk=id)
    return json.dumps(views.CustomUserView.render(None, record))


def get_movie(conn, id):
    record = models.Movie.objects.get(pk=id)
    return json.dumps(views.CustomMovieView.render(None, record))


def get_person(conn, id):
    record = models.Person.objects.get(pk=id)
    return json.dumps(views.CustomPersonView.render(None, record))


def update_movie(conn, id):
    record = models.Movie.objects.get(pk=id)
    record.title = f'{record.title}---{record.id}'
    record.save()
    return json.dumps({
        'id': record.id,
        'title': record.title,
    })


def insert_user(conn, val):
    num = random.randrange(1_000_000)
    record = models.User.objects.create(
        name=f'{val}{num}', image=f'image_{val}{num}')
    record.save()
    return json.dumps({
        'id': record.id,
        'name': record.name,
        'image': record.image,
    })


def setup(ctx, conn, queryname):
    if queryname == 'update_movie':
        with connection.cursor() as cur:
            cur.execute('''
                UPDATE
                    _django_movie
                SET
                    title = split_part(_django_movie.title, '---', 1)
                WHERE
                    _django_movie.title LIKE '%---%';
            ''')
    elif queryname == 'insert_user':
        with connection.cursor() as cur:
            cur.execute('''
                DELETE FROM
                    _django_user
                WHERE
                    _django_user.name LIKE %s
            ''', [f'{INSERT_PREFIX}%'])


def cleanup(ctx, conn, queryname):
    if queryname in {'update_movie', 'insert_user'}:
        # The clean up is the same as setup for mutation benchmarks
        setup(ctx, conn, queryname)
