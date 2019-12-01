import Browser
import Html exposing (..)
import Html.Attributes exposing (style)
import Html.Events exposing (onClick)
import Http
import Json.Decode as Decode exposing (..)
import Bootstrap.Button as Button
import Bootstrap.CDN as CDN
import Bootstrap.Grid as Grid


-- MODEL


type alias Stats =
  { processed : Int
  , failed : Int
  , retries : Int
  }


-- type alias Retries =
--   { retryCount : Int
--   , queue : String
--   , error : String
--   }


type alias Model =
  { stats : List Stats
  , errorMessage : Maybe String
  }



-- VIEW


view : Model -> Html Msg
view model =
  Grid.container []
    [ CDN.stylesheet
      , Button.button [ Button.primary, Button.onClick SendHttpRequest ] [ text "reload" ]
    , viewStatsOrError model
    ]


viewStatsOrError : Model -> Html Msg
viewStatsOrError model =
  case model.errorMessage of
    Just message ->
      viewError message

    Nothing ->
      viewStats model.stats


viewError : String -> Html Msg
viewError errorMessage =
  let
    errorHeading =
      "Couldn't fetch data at this time."
  in
  div []
    [ h3 [] [ text errorHeading ]
    , text ("Error: " ++ errorMessage)
    ]


viewTableHeader : Html Msg
viewTableHeader =
  div [ style "border" "1px solid #dddddd" ]
    [ tr []
        [ th [ style "padding" "8px" ]
          [ text "Processed" ]
        , th [ style "padding" "8px" ]
          [ text "Failed" ]
        , th [ style "padding" "8px" ]
          [ text "Retries" ]
        ]
    ]


viewStats : List Stats -> Html Msg
viewStats stats =
    tr []
      [ h4 [ style "padding" "8px"
           , style "font-family" "arial, sans-serif"
           ]
           [ text "Go Workers 2"
           ]
      , div []
        (List.map viewStatsData stats ++ [ viewTableHeader ])
      ]


viewStatsData : Stats -> Html Msg
viewStatsData stats =
  div [ style "border" "1px solid #dddddd"
      , style "color" "red" ]
      [ tr []
          [ td [ style "padding" "8px" ]
            [ text (String.fromInt stats.processed) ]
          , td [ style "padding" "8px" ]
            [ text (String.fromInt stats.failed) ]
          , td [ style "padding" "8px" ]
            [ text (String.fromInt stats.retries) ]
          ]
      ]


-- viewRetries : List Retries -> Html Msg
-- viewRetries retries =
--   tr []
--     [ h4 [ style "padding" "8px"
--          , style "font-family" "arial, sans-serif"
--          , style "border" "1px solid #dddddd"
--          ]
--          [ text "Retries"
--          ]
--     , div []
--       (List.map viewRetryData retries)
--     ]


-- viewRetryData : Retries -> Html Msg
-- viewRetryData retries =
--   div []
--     [ tr []
--         [ td[]
--           [ text ("test retry 1") ]
--         , td[]
--           [ text ("test retry 2") ]
--         , td[]
--           [ text ("test retry 3") ]
--         ]
--     ]


type Msg
  = SendHttpRequest
  | DataReceived (Result Http.Error (List Stats))



-- DECODER


statsDecoder : Decoder Stats
statsDecoder =
  map3 Stats
    (field "processed" int)
    (field "failed" int)
    (field "retries" int)



-- HTTP


httpCommand : Cmd Msg
httpCommand =
  Http.get
    { url = "http://localhost:8080/stats"
    , expect = Http.expectJson DataReceived (list statsDecoder)
    }



-- UPDATE


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
  case msg of
    SendHttpRequest ->
      ( model, httpCommand )

    DataReceived (Ok stats) ->
      ( { model
          | stats = stats
          , errorMessage = Nothing
        }
      , Cmd.none
      )

    DataReceived (Err httpError) ->
      ( { model
          | errorMessage = Just (buildErrorMessage httpError)
        }
      , Cmd.none
      )


buildErrorMessage : Http.Error -> String
buildErrorMessage httpError =
  case httpError of
    Http.BadUrl message ->
      message

    Http.Timeout ->
      "Server is taking too long to respond. Please try again later."

    Http.NetworkError ->
      "Unable to reach server."

    Http.BadStatus statusCode ->
      "Request failed with status code: " ++ String.fromInt statusCode

    Http.BadBody message ->
      message



-- MAIN


init : () -> ( Model, Cmd Msg )
init _ =
  ( { stats = []
    , errorMessage = Nothing
    }
  , Cmd.none
  )


main : Program () Model Msg
main =
  Browser.element
    { init = init
    , view = view
    , update = update
    , subscriptions = \_ -> Sub.none
    }
