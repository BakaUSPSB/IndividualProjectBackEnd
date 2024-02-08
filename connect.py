from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

# Replace these with your MySQL database credentials
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sakila'
}


@app.route('/topmovies')
def get_top_movies():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # Change this query according to your database schema
        query = "SELECT film_id, title FROM film ORDER BY rental_rate DESC LIMIT 5"
        cursor.execute(query)

        top_movies = cursor.fetchall()

        return jsonify(top_movies)

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Unable to fetch top movies"}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


@app.route('/topactors')
def get_actor():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # Change this query according to your database schema
        query = """SELECT a.actor_id,CONCAT(a.first_name, ' ', a.last_name) AS actor_name,SUM(f.rental_rate) AS total_rental_rate
                    FROM actor AS a
                    JOIN film_actor AS fa ON a.actor_id = fa.actor_id
                    JOIN film AS f ON fa.film_id = f.film_id
                    GROUP BY a.actor_id, actor_name
                    ORDER BY total_rental_rate DESC
                    LIMIT 5;
                """
        cursor.execute(query)

        top_actors = cursor.fetchall()

        return jsonify(top_actors)

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Unable to fetch top movies"}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


@app.route("/moreinfo/<int:movie_id>")
def get_movie_details(movie_id):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        query = """
            SELECT f.film_id, f.title, f.description, f.release_year, f.length, GROUP_CONCAT(CONCAT(a.first_name, ' ', a.last_name) SEPARATOR ', ') AS actors,GROUP_CONCAT(DISTINCT c.name SEPARATOR ', ') AS categories
            FROM film AS f
            JOIN film_actor AS fa ON f.film_id = fa.film_id
            JOIN actor AS a ON fa.actor_id = a.actor_id
            JOIN film_category AS fc ON f.film_id = fc.film_id
            JOIN category AS c ON fc.category_id = c.category_id
            WHERE f.film_id = %s
            GROUP BY f.film_id;
        """
        cursor.execute(query, (movie_id,))
        movie = cursor.fetchone()

        if movie:
            movie_details = {
                "film_id": movie['film_id'],
                "title": movie['title'],
                "description": movie['description'],
                "release_year": movie['release_year'],
                "length": movie['length'],
                "actors": movie['actors'],
                "genre": movie['categories'],
                # Add more movie details here
            }
            return jsonify(movie_details)
        else:
            return jsonify({"error": "Movie not found"}), 404
    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Unable to fetch movie details"}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


@app.route("/moreactor/<int:actor_id>")
def get_actor_details(actor_id):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        query = """
                SELECT f.film_id, f.title, f.description, f.release_year, f.length, SUM(f.rental_rate) AS total_rental_score
                FROM film AS f
                JOIN film_actor AS fa ON f.film_id = fa.film_id
                WHERE fa.actor_id = %s 
                GROUP BY f.film_id, f.title, f.description, f.release_year, f.length
                ORDER BY total_rental_score DESC
                LIMIT 5;
        """
        cursor.execute(query, (actor_id,))
        actors = cursor.fetchall()  # Fetch all results

        actor_details = {
            "actor_id": actor_id,
            "top_movies": []
        }

        for actor in actors:
            actor_details["top_movies"].append({
                "film_id": actor['film_id'],
                "title": actor['title'],
                "description": actor['description'],
                "release_year": actor['release_year'],
                "length": actor['length'],
                "total_rental_score": actor['total_rental_score']
            })

        return jsonify(actor_details)

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Unable to fetch movie details"}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == '__main__':
    app.run(debug=True)
